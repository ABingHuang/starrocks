// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class InsertOverwriteJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJob.class);

    @SerializedName(value = "jobId")
    private long jobId;

    public enum OverwriteJobState {
        PENDING,
        PREPARED,
        LOADING,
        COMMITTING,
        SUCCESS,
        FAILED,
        CANCELLED
    }
    @SerializedName(value = "jobState")
    private AtomicReference<OverwriteJobState> jobState;

    @SerializedName(value = "sourcePartitionNames")
    private List<String> sourcePartitionNames;

    @SerializedName(value = "newPartitionNames")
    private List<String> newPartitionNames;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "targetTableId")
    private long targetTableId;

    @SerializedName(value = "targetTableName")
    private String targetTableName;

    @SerializedName(value = "targetPartitionIds")
    private Set<Long> targetPartitionIds;

    private long watershedTxnId = -1;
    // isOverwrite is false, changed to insert into
    private InsertStmt insertStmt;
    private ConnectContext context;
    // used to cancel job
    private Object cv;

    private boolean isCancelled = false;

    public InsertOverwriteJob(ConnectContext context, long jobId, InsertStmt insertStmt,
                              long dbId, long targetTableId, String targetTableName, Set<Long> targetPartitionIds) {
        this.context = context;
        this.jobId = jobId;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.insertStmt = insertStmt;
        this.dbId = dbId;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.targetPartitionIds = targetPartitionIds;
        this.cv = new Object();
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long dbId, long targetTableId, String targetTableName, Set<Long> targetPartitionIds) {
        // this.context = new ConnectContext();
        // context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        this.jobId = jobId;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.dbId = dbId;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.targetPartitionIds = targetPartitionIds;
        this.cv = new Object();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static InsertOverwriteJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        InsertOverwriteJob job = GsonUtils.GSON.fromJson(json, InsertOverwriteJob.class);
        return job;
    }

    public long getTargetDbId() {
        return dbId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public Set<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    public OverwriteJobState getJobState() {
        return jobState.get();
    }

    public boolean isFinished() {
        return jobState.get() == OverwriteJobState.SUCCESS
                || jobState.get() == OverwriteJobState.FAILED
                || jobState.get() == OverwriteJobState.CANCELLED;
    }

    public boolean cancel() {
        try {
            isCancelled = true;
            if (cv != null) {
                synchronized (cv) {
                    cv.notifyAll();
                }
            }
            transferTo(OverwriteJobState.CANCELLED);
        } catch (Exception e) {
            LOG.warn("cancel insert overwrite job:{} failed", jobId, e);
            return false;
        }
        return true;
    }

    /*
    @Override
    public void gsonPostProcess() throws IOException {
        this.context = new ConnectContext();
        this.cv = new Object();
    }

     */

    public OverwriteJobState run() {
        handle();
        return jobState.get();
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
                sourcePartitionNames = info.getSourcePartitionNames();
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
                LOG.info("replay insert overwrite job:{} to FAILED", jobId);
                gc();
                break;
            case CANCELLED:
                jobState.set(OverwriteJobState.CANCELLED);
                LOG.info("replay insert overwrite job:{} to CANCELLED", jobId);
                gc();
                break;
            case SUCCESS:
                jobState.set(OverwriteJobState.SUCCESS);
                LOG.info("replay insert overwrite job:{} to SUCCESS", jobId);
                break;
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
                executeInsert();
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
                new InsertOverwriteStateChangeInfo(jobId, jobState.get(), state,
                        sourcePartitionNames, newPartitionNames);
        LOG.info("InsertOverwriteStateChangeInfo:{}", info);
        Catalog.getCurrentCatalog().getEditLog().logInsertOverwriteStateChange(info);
        jobState.set(state);
        handle();
    }

    private void prepare() {
        Preconditions.checkState(jobState.get() == OverwriteJobState.PENDING);
        try {
            this.watershedTxnId =
                    Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            createTempPartitions();
            transferTo(OverwriteJobState.PREPARED);
        } catch (Exception e) {
            LOG.warn("prepare insert overwrite job:{} failed.", jobId, e);
            transferTo(OverwriteJobState.FAILED);
        }
    }

    // export transaction id
    private void executeInsert() {
        Preconditions.checkState(jobState.get() == OverwriteJobState.LOADING);
        LOG.info("start to execute insert");
        insertStmt.setOverwrite(false);
        try {
            StmtExecutor stmtExecutor = new StmtExecutor(context, insertStmt);
            stmtExecutor.execute();
            LOG.info("execute insert finished");
            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                // ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
                // drop temp partitions
                LOG.warn("execute insert failed, jobId:{}", jobId);
                // throw new RuntimeException("insert failed");
                transferTo(OverwriteJobState.FAILED);
                return;
            }
            transferTo(OverwriteJobState.COMMITTING);
            // init loadFuture and add listener
        } catch (Throwable t) {
            LOG.warn("insert overwrite job:{} failed", jobId, t);
        }
    }

    private void createTempPartitions() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db[{}] does not exist", dbId);
            throw new RuntimeException("db does not exist");
        }
        OlapTable targetTable;
        db.readLock();
        try {
            targetTable = (OlapTable) db.getTable(targetTableId);
            if (targetTable == null) {
                LOG.warn("table:{} tableId[{}] does not exist in db:{}", targetTableName, targetTableId, dbId);
                throw new RuntimeException("table does not exist");
            }
        } finally {
            db.readUnlock();
        }
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
            List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(newTempPartitions.size());
            for (int i = 0; i < newTempPartitions.size(); i++) {
                targetTable.addTempPartition(newTempPartitions.get(i));
                long sourcePartitionId = sourcePartitions.get(i).getId();
                partitionInfo.addPartition(newTempPartitions.get(i).getId(),
                        partitionInfo.getDataProperty(sourcePartitionId),
                        partitionInfo.getReplicationNum(sourcePartitionId),
                        partitionInfo.getIsInMemory(sourcePartitionId));
                Partition partition = newTempPartitions.get(i);
                // range is null for UNPARTITIONED type
                Range<PartitionKey> range = null;
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    rangePartitionInfo.setRange(partition.getId(), true,
                            rangePartitionInfo.getRange(sourcePartitionId));
                    range = rangePartitionInfo.getRange(partition.getId());
                }
                // contruct PartitionPersistInfo
                PartitionPersistInfo info =
                        new PartitionPersistInfo(db.getId(), targetTable.getId(), partition,
                                range,
                                partitionInfo.getDataProperty(partition.getId()),
                                partitionInfo.getReplicationNum(partition.getId()),
                                partitionInfo.getIsInMemory(partition.getId()),
                                true);
                partitionInfoList.add(info);
            }
            AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
            LOG.info("add AddPartitionsInfo log");
            Catalog.getCurrentCatalog().getEditLog().logAddPartitions(infos);
            LOG.info("create temp partition finished");
        } finally {
            db.writeUnlock();
        }
    }

    private void gc() {
        LOG.info("start to garbage collect");
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db[{}] does not exist", dbId);
            throw new RuntimeException("db does not exist");
        }
        db.writeLock();
        try {
            OlapTable targetTable = (OlapTable) db.getTable(targetTableId);
            if (targetTable == null) {
                LOG.warn("tablet:{} tableId[{}] does not exist in db:{}", targetTableName, targetTableId, dbId);
                throw new RuntimeException("table does not exist");
            }
            if (newPartitionNames != null) {
                for (String partitionName : newPartitionNames) {
                    LOG.info("drop partition:{}", partitionName);

                    Partition partition = targetTable.getPartition(partitionName, true);
                    if (partition != null) {
                        targetTable.dropTempPartition(partitionName, true);
                    } else {
                        LOG.warn("partition is null for name:{}", partitionName);
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    private void commit() {
        Preconditions.checkState(jobState.get() == OverwriteJobState.COMMITTING);
        LOG.info("start to commit insert overwrite job:{}", jobId);
        try {
            LOG.info("start to sleep in commit");
            Thread.sleep(20000);
            LOG.info("finish sleep in commit");
            boolean ret = doCommit();
            if (ret) {
                transferTo(OverwriteJobState.SUCCESS);
                LOG.info("commit success, jobId:{}", jobId);
                return;
            }
        } catch (Exception exp) {
            LOG.warn("commit failed. there maybe some serious errors");
            transferTo(OverwriteJobState.FAILED);
        }
    }

    private boolean doCommit() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db:{} does not exist", dbId);
            throw new RuntimeException("db does not exist");
        }
        db.writeLock();
        try {
            OlapTable targetTable = (OlapTable) db.getTable(targetTableId);
            if (targetTable == null) {
                LOG.warn("tablet:{} tableId[{}] does not exist in db:{}", targetTableName, targetTableId, dbId);
                throw new RuntimeException("table does not exist");
            }
            if (targetTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                targetTable.replaceTempPartitions(sourcePartitionNames, newPartitionNames, true, false);
            } else {
                LOG.info("replace source partition:{} with temp partition:{}",
                        sourcePartitionNames.get(0), newPartitionNames.get(0));
                targetTable.replacePartition(sourcePartitionNames.get(0), newPartitionNames.get(0));
            }
        } catch (Exception e) {
            LOG.warn("replace partitions failed when insert overwrite into tableId:{}, table:{}",
                    targetTableId, targetTableName, e);
            return false;
        } finally {
            db.writeUnlock();
        }
        return true;
    }

    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return Catalog.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(targetTableId));
    }

    private void startLoad() {
        Preconditions.checkState(jobState.get() == OverwriteJobState.PREPARED);
        Preconditions.checkState(insertStmt != null);
        try {
            // modify all the partitions in insertStmt
            LOG.info("start to load, jobId:{}", jobId);
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                LOG.warn("db:{} does not exist", dbId);
                throw new RuntimeException("db does not exist");
            }
            db.readLock();
            try {
                OlapTable targetTable = (OlapTable) db.getTable(targetTableId);
                if (targetTable == null) {
                    LOG.warn("table:{} tableId[{}] does not exist in db:{}", targetTableName, targetTableId, dbId);
                    throw new RuntimeException("table does not exist");
                }
                List<Long> newPartitionIds = newPartitionNames.stream()
                        .map(partitionName -> targetTable.getPartition(partitionName, true).getId())
                        .collect(Collectors.toList());
                LOG.info("newPartitionIds:{}",
                        Strings.join(newPartitionIds.stream().map(id -> id.toString()).collect(Collectors.toList()), ","));
                PartitionNames partitionNames = new PartitionNames(true, newPartitionNames);
                insertStmt.setTargetPartitionNames(partitionNames);
                insertStmt.setTargetPartitionIds(newPartitionIds);
            } finally {
                db.readUnlock();
            }

            LOG.info("start to wait previous load finish. watershedTxnId:{}", watershedTxnId);
            while (!isCancelled && !isPreviousLoadFinished()) {
                synchronized (cv) {
                    cv.wait(1000);
                }
            }
            LOG.info("wait finished. isCancelled:{}, isPreviousLoadFinished:{}",
                    isCancelled, isPreviousLoadFinished());
            transferTo(OverwriteJobState.LOADING);
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} failed in loading.", jobId, e);
            transferTo(OverwriteJobState.FAILED);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InsertOverwriteJob that = (InsertOverwriteJob) o;
        return jobId == that.jobId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }
}
