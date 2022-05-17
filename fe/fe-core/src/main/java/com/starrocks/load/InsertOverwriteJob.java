// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class InsertOverwriteJob {
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
    private InsertStmt insertStmt;
    private StmtExecutor stmtExecutor;
    private ConnectContext context;
    private Database db;
    private OlapTable targetTable;
    private String postfix;

    public InsertOverwriteJob(long jobId, ConnectContext context, StmtExecutor stmtExecutor,
                              InsertStmt insertStmt, Database db,
                              OlapTable targetTable, Set<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.context = context;
        this.stmtExecutor = stmtExecutor;
        this.insertStmt = insertStmt;
        this.db = db;
        this.targetTable = targetTable;
        this.targetPartitionIds = targetPartitionIds;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.dbId = db.getId();
        this.targetTableId = targetTable.getId();
        this.targetTableName = targetTable.getName();
        this.postfix = "_" + jobId;
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long dbId, long targetTableId, String targetTableName, Set<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.jobState = new AtomicReference<>(OverwriteJobState.PENDING);
        this.dbId = dbId;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.targetPartitionIds = targetPartitionIds;
        this.db = GlobalStateMgr.getCurrentState().getDb(dbId);
        this.targetTable = (OlapTable) db.getTable(targetTableId);
        this.postfix = "_" + jobId;
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

    // only called from log replay
    // there is no concurrent problem here
    public boolean cancel() {
        if (isFinished()) {
            LOG.warn("cancel failed. insert overwrite job:{} already finished. state:{}", jobState);
            return false;
        }
        try {
            transferTo(OverwriteJobState.CANCELLED);
        } catch (Exception e) {
            LOG.warn("cancel insert overwrite job:{} failed", jobId, e);
            return false;
        }
        return true;
    }

    public OverwriteJobState run() throws Exception {
        handle();
        return jobState.get();
    }

    public void handle() throws Exception {
        try {
            switch (jobState.get()) {
                case PENDING:
                    prepare();
                    break;
                case PREPARED:
                    doLoad();
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
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} handle exception", jobId, e);
            if (jobState.get() != OverwriteJobState.FAILED && jobState.get() != OverwriteJobState.CANCELLED) {
                transferTo(OverwriteJobState.FAILED);
            }
            throw e;
        }
    }

    private void doLoad() throws Exception {
        Preconditions.checkState(jobState.get() == OverwriteJobState.PREPARED);
        try {
            createTempPartitions();
            prepareInsert();
            executeInsert();
            doCommit();
            transferTo(OverwriteJobState.SUCCESS);
        } catch (Exception e) {
            LOG.info("insert overwrite job:{} load failed", jobId, e);
            throw e;
        }
    }

    public void replayStateChange(InsertOverwriteStateChangeInfo info) {
        LOG.info("replay state change:{}", info);
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
                doCommit();
                LOG.info("replay insert overwrite job:{} to SUCCESS", jobId);
                break;
            default:
                LOG.warn("invalid to state:{} for insert overwrite job:{}", info.getToState(), jobId);
        }
    }

    /*
    @Override
    public void gsonPostProcess() throws IOException {
        LOG.info("InsertOverwriteJob:{} gsonPostProcess called", jobId);
    }

     */

    private void transferTo(OverwriteJobState state) throws Exception {
        if (state == OverwriteJobState.SUCCESS) {
            Preconditions.checkState(jobState.get() == OverwriteJobState.PREPARED);
        }
        InsertOverwriteStateChangeInfo info =
                new InsertOverwriteStateChangeInfo(jobId, jobState.get(), state,
                        sourcePartitionNames, newPartitionNames);
        LOG.info("InsertOverwriteStateChangeInfo:{}", info);
        GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
        jobState.set(state);
        handle();
    }

    private void prepare() throws Exception {
        Preconditions.checkState(jobState.get() == OverwriteJobState.PENDING);
        try {
            this.watershedTxnId =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            List<Partition> sourcePartitions = insertStmt.getTargetPartitionIds().stream()
                    .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
            sourcePartitionNames = sourcePartitions.stream().map(p -> p.getName()).collect(Collectors.toList());
            newPartitionNames = sourcePartitionNames.stream().map(name -> name + postfix).collect(Collectors.toList());
            transferTo(OverwriteJobState.PREPARED);
        } catch (Exception e) {
            LOG.warn("prepare insert overwrite job:{} failed.", jobId, e);
            throw e;
        }
    }

    private void executeInsert() throws Exception {
        LOG.info("start to execute insert");
        ExecPlan newPlan = new StatementPlanner().plan(insertStmt, context);
        stmtExecutor.handleDMLStmt(newPlan, insertStmt);
        LOG.info("execute insert finished");
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            LOG.warn("execute insert failed, jobId:{}", jobId);
            throw new RuntimeException("execute insert failed");
        }
    }

    private void createTempPartitions() {
        try {
            List<Partition> newTempPartitions = GlobalStateMgr.getCurrentState().createTempPartitionsFromPartitions(
                    db, targetTable, postfix, targetPartitionIds);
            LOG.info("postfix:{}, sourcePartitionNames:{}, newPartitionNames:{}, newTempPartitions size:{}",
                    postfix, Strings.join(sourcePartitionNames, ","), Strings.join(newPartitionNames, ","),
                    newTempPartitions.size());
            db.writeLock();
            try {
                List<Partition> sourcePartitions = insertStmt.getTargetPartitionIds().stream()
                        .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
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
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
                LOG.info("create temp partition finished");
            } finally {
                db.writeUnlock();
            }
        } catch (Throwable t) {
            LOG.warn("create temp partitions failed", t);
            throw t;
        }
    }

    private void gc() {
        LOG.info("start to garbage collect");
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
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
        } catch (Exception e) {
            LOG.warn("exception when gc.", e);
        } finally {
            db.writeUnlock();
        }
    }

    /*
    private void commit() {
        LOG.info("start to commit insert overwrite job:{}", jobId);
        try {
            doCommit();
        } catch (Exception exp) {
            LOG.warn("commit failed. there maybe some serious errors", exp);
            throw exp;
        }
    }

     */

    private void doCommit() {
        db.writeLock();
        try {
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
            throw new RuntimeException("replace partitions failed", e);
        } finally {
            db.writeUnlock();
        }
    }

    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(targetTableId));
    }

    private void prepareInsert() throws Exception {
        Preconditions.checkState(jobState.get() == OverwriteJobState.PREPARED);
        Preconditions.checkState(insertStmt != null);
        try {
            LOG.info("start to load, jobId:{}", jobId);
            db.readLock();
            try {
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

            // wait the previous loads
            LOG.info("start to wait previous load finish. watershedTxnId:{}", watershedTxnId);
            while (!isPreviousLoadFinished() && !context.isKilled()) {
                Thread.sleep(500);
            }
            LOG.info("wait finished. isPreviousLoadFinished:{}, context.isKilled:{}",
                    isPreviousLoadFinished(), context.isKilled());
        } catch (Exception e) {
            LOG.warn("insert overwrite job:{} failed in loading.", jobId, e);
            throw e;
        }
    }
}
