// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

// 持久化
// 可重入
// 可取消
// failover
// failover依赖于partition，依赖于事务的replay
public class InsertOverwriteJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJob.class);

    @SerializedName(value = "jobId")
    private long jobId;
    // isOverwrite is false, changed to insert into
    private InsertStmt insertStmt;
    private ConnectContext context;
    // can get from insertStmt
    @SerializedName(value = "targetTable")
    private OlapTable targetTable;

    // private List<Partition> targetPartitions;

    public enum OverwriteJobState {
        PENDING,
        PREPARED,
        LOADING,
        COMMITTING,
        SUCCESS,
        FAILED,
        CANCELLED
    }

    private AtomicReference<OverwriteJobState> jobState;

    List<String> sourcePartitionNames;
    List<String> newPartitionNames;
    Database db;
    private ExecutorService loadExecutorService;
    private Future<Void> loadFuture;

    public InsertOverwriteJob(ConnectContext context, long jobId, InsertStmt insertStmt) {
        this.context = context;
        this.jobId = jobId;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.insertStmt = insertStmt;
        this.targetTable = (OlapTable) insertStmt.getTargetTable();
        this.db = Catalog.getCurrentCatalog().getDb(insertStmt.getDb());
        this.loadExecutorService = Executors.newCachedThreadPool();
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long dbId, long tableId, String originInsertSql) throws AnalysisException {
        this.context = new ConnectContext();
        context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        this.jobId = jobId;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.db = Catalog.getCurrentCatalog().getDb(dbId);
        this.targetTable = (OlapTable) db.getTable(tableId);

        // TODO: optimize this
        List<StatementBase> stmts;
        try {
            stmts = com.starrocks.sql.parser.SqlParser.parse(originInsertSql, context.getSessionVariable().getSqlMode());
        } catch (ParsingException parsingException) {
            LOG.warn("parse sql error. originInsertSql:{}", originInsertSql, parsingException);
            throw new AnalysisException(parsingException.getMessage());
        }
        this.insertStmt = (InsertStmt) stmts.get(0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public long getTargetDbId() {
        return db.getId();
    }

    public long getTargetTableId() {
        return targetTable.getId();
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getTargetTableName() {
        return targetTable.getName();
    }

    public List<Long> getTargetPartitionIds() {
        return insertStmt.getTargetPartitionIds();
    }

    public boolean cancel() {
        try {
            transferTo(OverwriteJobState.CANCELLED);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public OverwriteJobState run() {
        handle();
        return jobState.get();
    }

    public OverwriteJobState restart() {
        LOG.info("restart insert overwrite job:{}", jobId);
        gc();
        jobState.set(OverwriteJobState.PENDING);
        return run();
    }

    public void replayStateChange(InsertOverwriteStateChangeInfo info) {
        LOG.info("replay state change");
        if (info.getFromState() != jobState.get()) {
            LOG.warn("invalid job info. current state:{}, from state:{}", jobState, info.getFromState());
            return;
        }
        // state can not be PENDING here
        switch (info.getToState()) {
            case PREPARED:
                newPartitionNames = info.getNewPartitionsName();
                jobState.set(OverwriteJobState.PREPARED);
                break;
            case LOADING:
                // 这里依赖于事务的完成
                // 这里先做成重试的逻辑，后续可以考虑优化成重试导入
                jobState.set(OverwriteJobState.LOADING);
                break;
            case COMMITTING:
                jobState.set(OverwriteJobState.COMMITTING);
                doCommit();
                break;
            case FAILED:
                jobState.set(OverwriteJobState.FAILED);
                gc();
                break;
            case CANCELLED:
                jobState.set(OverwriteJobState.CANCELLED);
                gc();
                break;
            case SUCCESS:
                LOG.info("replay insert overwrite job:{} succeed", jobId);
            default:
                LOG.warn("invalid state:{} for insert overwrite job:{}", jobState, jobId);
        }
    }

    public void handle() {
        switch (jobState.get()) {
            case PENDING:
                prepare();
                break;
            case PREPARED:
                startLoad();
                break;
            case LOADING:
                waitLoad();
                break;
            case COMMITTING:
                commit();
                break;
            case FAILED:
            case CANCELLED:
                gc();
                break;
            case SUCCESS:
                LOG.info("insert overwrite job:{} succeed", jobId);
                break;
            default:
                throw new RuntimeException("invalid jobState:" + jobState);
        }
    }

    private void transferTo(OverwriteJobState state) {
        InsertOverwriteStateChangeInfo info =
                new InsertOverwriteStateChangeInfo(jobId, jobState.get(), state, newPartitionNames);
        Catalog.getCurrentCatalog().getEditLog().logInsertOverwriteStateChange(info);
        jobState.set(state);
        handle();
    }

    private void prepare() {
        if (jobState.get() != OverwriteJobState.PENDING) {
            LOG.warn("invalid job state:{} to prepare", jobState);
            return;
        }
        try {
            createTempPartitions();
            transferTo(OverwriteJobState.PREPARED);
        } catch (Exception e) {
            LOG.warn("prepare insert overwrite job:{} failed.", jobId, e);
            transferTo(OverwriteJobState.FAILED);
        }
    }

    // export transaction id
    private void executeInsert() {
        LOG.info("start to execute insert");
        insertStmt.setOverwrite(false);
        try {
            StmtExecutor stmtExecutor = new StmtExecutor(context, insertStmt);
            stmtExecutor.execute();
            LOG.info("execute insert finished");
            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                // ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
                // drop temp partitions
                LOG.info("execute insert failed, jobId:{}", jobId);
                throw new RuntimeException("insert failed");
            }
            // init loadFuture and add listener
        } catch (Throwable t) {
            LOG.warn("insert overwrite job:{} failed", jobId, t);
            // ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
            throw new RuntimeException("insert overwrite job failed", t);
        }
    }

    private void createTempPartitions() {
        // make temp partitions for targetPartitions
        String postfix = "_" + jobId;
        List<Partition> sourcePartitions = insertStmt.getTargetPartitionIds().stream()
                .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
        sourcePartitionNames = sourcePartitions.stream().map(p -> p.getName()).collect(Collectors.toList());
        newPartitionNames = sourcePartitionNames.stream().map(name -> name + postfix).collect(Collectors.toList());
        List<Partition> newTempPartitions = Catalog.getCurrentCatalog().createTempPartitionsFromPartitions(
                db, targetTable, postfix, insertStmt.getTargetPartitionIds());
        LOG.info("postfix:{}, sourcePartitionNames:{}, newPartitionNames:{}, newTempPartitions size:{}",
                postfix, Strings.join(sourcePartitionNames, ","), Strings.join(newPartitionNames, ","),
                newTempPartitions.size());
        db.writeLock();
        try {
            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (int i = 0; i < newTempPartitions.size(); i++) {
                    targetTable.addTempPartition(newTempPartitions.get(i));

                    long sourcePartitionId = sourcePartitions.get(i).getId();
                    rangePartitionInfo.setRange(newTempPartitions.get(i).getId(), true,
                            rangePartitionInfo.getRange(sourcePartitionId));
                    rangePartitionInfo.addPartition(newTempPartitions.get(i).getId(),
                            rangePartitionInfo.getDataProperty(sourcePartitionId),
                            rangePartitionInfo.getReplicationNum(sourcePartitionId),
                            rangePartitionInfo.getIsInMemory(sourcePartitionId));
                }
                List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(newTempPartitions.size());
                for (int i = 0; i < newTempPartitions.size(); i++) {
                    Partition partition = newTempPartitions.get(i);
                    PartitionPersistInfo info =
                            new PartitionPersistInfo(db.getId(), targetTable.getId(), partition,
                                    rangePartitionInfo.getRange(partition.getId()),
                                    rangePartitionInfo.getDataProperty(partition.getId()),
                                    rangePartitionInfo.getReplicationNum(partition.getId()),
                                    rangePartitionInfo.getIsInMemory(partition.getId()),
                                    true);
                    partitionInfoList.add(info);
                }

                AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                Catalog.getCurrentCatalog().getEditLog().logAddPartitions(infos);
            } else {
                throw new RuntimeException("do not support unpartitioned type table");
            }

            LOG.info("create temp partition finished");
        } finally {
            db.writeUnlock();
        }
    }

    private void gc() {
        LOG.info("start to garbage collect");
        if (loadFuture != null) {
            boolean ret = loadFuture.cancel(true);
            if (!ret) {
                LOG.warn("cancel load thread failed");
            }
        }
        db.writeLock();
        try {
            // pay attention to failover and edit log info
            if (newPartitionNames != null) {
                for (String partitionName : newPartitionNames) {
                    targetTable.dropTempPartition(partitionName, true);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    private void waitLoad() {
        try {
            loadFuture.get();
            transferTo(OverwriteJobState.COMMITTING);
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} load failed", jobId, e);
            transferTo(OverwriteJobState.FAILED);
        }
    }

    private void commit() {
        if (jobState.get() != OverwriteJobState.COMMITTING) {
            LOG.warn("invalid job state:{} to finish", jobState);
            return;
        }
        // try 3 times, or failed
        for (int i = 0; i < 3; i++) {
            try {
                boolean ret = doCommit();
                if (ret) {
                    transferTo(OverwriteJobState.SUCCESS);
                    LOG.info("commit success, jobId:{}", jobId);
                    return;
                }
            } catch (Exception exp) {
                LOG.warn("transferToSuccess failed. will retry", exp);
            }
        }
        LOG.warn("commit failed. there maybe some serious errors");
        transferTo(OverwriteJobState.FAILED);
    }

    private boolean doCommit() {
        // swap
        db.writeLock();
        try {
            // pay attention to log info
            targetTable.replaceTempPartitions(sourcePartitionNames, newPartitionNames, true, false);
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into tableId:{}, tableName:{}",
                    targetTable.getId(), targetTable.getName(), e);
            return false;
        } finally {
            db.writeUnlock();
        }
        return true;
    }

    private void startLoad() {
        if (jobState.get() != OverwriteJobState.PREPARED) {
            LOG.warn("invalid job state:{} to start load", jobState);
            return;
        }
        try {
            // modify all the partitions in insertStmt
            LOG.info("start to load, jobId:{}", jobId);
            db.readLock();
            try {
                List<Long> newPartitionIds = newPartitionNames.stream()
                        .map(partitionName -> targetTable.getPartition(partitionName, true).getId()).collect(Collectors.toList());
                LOG.info("newPartitionIds:{}",
                        Strings.join(newPartitionIds.stream().map(id -> id.toString()).collect(Collectors.toList()), ","));
                insertStmt.setTargetPartitionIds(newPartitionIds);
            } finally {
                db.readUnlock();
            }

            // execute insert stmt
            loadFuture = CompletableFuture.runAsync(() -> {
                executeInsert();
            }, loadExecutorService);
            transferTo(OverwriteJobState.LOADING);
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} failed in loading.", jobId, e);
            transferTo(OverwriteJobState.FAILED);
        }
    }
}
