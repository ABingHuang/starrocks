// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import io.netty.util.concurrent.DefaultThreadFactory;
import jersey.repackaged.com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertOverwriteJobManager implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobManager.class);

    @SerializedName(value = "overwriteJobMap")
    private Map<Long, InsertOverwriteJob> overwriteJobMap;

    // tableId -> partitionId list
    @SerializedName(value = "partitionsWithOverwrite")
    private Map<Long, Set<Long>> partitionsWithOverwrite;

    @SerializedName(value = "jobNum")
    private long jobNum;

    private ExecutorService cancelJobExecutorService;

    private List<InsertOverwriteJob> runningJobs;

    private Map<Long, Long> jobToTxnId;

    private ReentrantReadWriteLock lock;



    public InsertOverwriteJobManager() {
        this.overwriteJobMap = Maps.newHashMap();
        this.partitionsWithOverwrite = Maps.newHashMap();
        ThreadFactory threadFactory = new DefaultThreadFactory("cancel-thread");
        this.cancelJobExecutorService = Executors.newSingleThreadExecutor(threadFactory);
        this.runningJobs = Lists.newArrayList();
        this.lock = new ReentrantReadWriteLock();
        this.jobToTxnId = Maps.newHashMap();
        this.jobNum = 0;
    }

    public InsertOverwriteJob getOverwriteJob(long jobId) {
        lock.readLock().lock();
        try {
            return overwriteJobMap.get(jobId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean submitJob(InsertOverwriteJob job) throws Exception {
        try {
            boolean registered = registerOverwriteJob(job);
            if (!registered) {
                throw new RuntimeException("register insert overwrite job failed");
            }
            InsertOverwriteJob.OverwriteJobState state = job.run();
            return state == InsertOverwriteJob.OverwriteJobState.OVERWRITE_SUCCESS;
        } finally {
            deregisterOverwriteJob(job.getJobId());
        }
    }

    public void registerOverwriteJobTxn(long jobId, long txnId) {
        lock.writeLock().lock();
        try {
            LOG.info("overwrite jobId:{} register txnId:{}", jobId, txnId);
            jobToTxnId.put(jobId, txnId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean registerOverwriteJob(InsertOverwriteJob job) {
        lock.writeLock().lock();
        try {
            LOG.info("start to register job:{}", job.getJobId());
            if (overwriteJobMap.containsKey(job.getJobId())) {
                LOG.warn("job:{} is running", job.getJobId());
                return false;
            }
            // check whether there is a overwrite job running in partitions
            if (hasRunningOverwriteJob(-1, job.getTargetTableId(), job.getTargetPartitionIds())) {
                LOG.warn("table:{} has running overwrite jobs", job.getTargetTableId());
                return false;
            }
            overwriteJobMap.put(job.getJobId(), job);
            Set<Long> runningPartitions = partitionsWithOverwrite.getOrDefault(job.getTargetTableId(), Sets.newHashSet());
            if (job.getTargetPartitionIds() != null) {
                runningPartitions.addAll(job.getTargetPartitionIds());
            } else {
                LOG.info("job:{} target partition ids is null", job.getJobId());
            }
            partitionsWithOverwrite.put(job.getTargetTableId(), runningPartitions);
            LOG.info("register overwrite job:{} success", job.getJobId());
            jobNum++;
            return true;
        } catch (Exception  e) {
            LOG.warn("register overwrite job failed", e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean deregisterOverwriteJob(long jobid) {
        LOG.info("start to deregister overwrite job:{}", jobid);
        lock.writeLock().lock();
        try {
            if (!overwriteJobMap.containsKey(jobid)) {
                return true;
            }
            InsertOverwriteJob job = overwriteJobMap.get(jobid);
            jobToTxnId.remove(jobid);
            Set<Long> partitionIds = partitionsWithOverwrite.get(job.getTargetTableId());
            LOG.info("deregister partitions:{}", partitionIds);
            if (partitionIds != null) {
                partitionIds.removeAll(job.getTargetPartitionIds());
                if (partitionIds.isEmpty()) {
                    partitionsWithOverwrite.remove(job.getTargetTableId());
                }
            } else {
                partitionsWithOverwrite.remove(job.getTargetTableId());
            }
            jobNum--;
            return true;
        } catch (Exception e) {
            LOG.warn("deregister overwrite job failed", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(long txnId, long tableId) {
        lock.readLock().lock();
        try {
            if (jobToTxnId.values().contains(txnId)) {
                // overwrite job txn will return false
                LOG.info("txnId:{} is overwrite job for table:{}, return false", txnId, tableId);
                return false;
            }
            return partitionsWithOverwrite.containsKey(tableId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(long txnId, long tableId, Set<Long> partitions) {
        lock.readLock().lock();
        try {
            if (jobToTxnId.values().contains(txnId)) {
                // overwrite job txn will return false
                LOG.info("txnId:{} is overwrite job for table:{}, return false", txnId, tableId);
                return false;
            }
            boolean tableExist = partitionsWithOverwrite.containsKey(tableId);
            if (!tableExist) {
                return false;
            }
            if (partitions == null) {
                // means check table
                return true;
            }
            Set<Long> runningPartitions = partitionsWithOverwrite.get(tableId);
            if (runningPartitions == null || runningPartitions.isEmpty()) {
                return true;
            }
            if (partitions.stream().anyMatch(pId -> runningPartitions.contains(pId))) {
                return true;
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void replayCreateInsertOverwrite(CreateInsertOverwriteJobInfo jobInfo) {
        try {
            InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(jobInfo.getJobId(),
                    jobInfo.getDbId(), jobInfo.getTableId(), jobInfo.getTableName(), jobInfo.getTargetPartitionIds());
            boolean registered = registerOverwriteJob(insertOverwriteJob);
            if (!registered) {
                LOG.warn("register insert overwrite job failed. jobId:{}", insertOverwriteJob.getJobId());
                return;
            }
            if (runningJobs == null) {
                runningJobs = Lists.newArrayList();
            }
            runningJobs.add(insertOverwriteJob);
        } catch (Exception e) {
            LOG.warn("replay insert overwrite job failed. jobId:{}", jobInfo.getJobId(), e);
        }
    }

    public void replayInsertOverwriteStateChange(InsertOverwriteStateChangeInfo info) {
        InsertOverwriteJob job = getOverwriteJob(info.getJobId());
        job.replayStateChange(info);
        if (job.isFinished()) {
            LOG.info("insert overwrite job is finished. deregister it and remove from runningJobs");
            deregisterOverwriteJob(job.getJobId());
            if (runningJobs != null) {
                runningJobs.remove(job);
            }
        }
    }

    public void cancelRunningJobs() {
        // resubmit running insert overwrite jobs
        if (!GlobalStateMgr.isCheckpointThread()) {
            cancelJobExecutorService.submit(() -> {
                // wait until serving catalog is ready
                while (!GlobalStateMgr.getServingState().isReady()) {
                    try {
                        // not return, but sleep a while. to avoid some thread with large running interval will
                        // wait for a long time to start again.
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.warn("InsertOverwriteJobManager runAfterCatalogReady interrupted exception.", e);
                    }
                }
                LOG.info("start to cancel running jobs, size:{}", runningJobs.size());
                if (runningJobs != null) {
                    for (InsertOverwriteJob job : runningJobs) {
                        LOG.info("start to gc insert overwrite job:{}", job.getJobId());
                        try {
                            job.cancel();
                        } finally {
                            deregisterOverwriteJob(job.getJobId());
                        }
                    }
                    runningJobs = null;
                }
            });
        }
    }

    public long getJobNum() {
        return jobNum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InsertOverwriteJobManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        InsertOverwriteJobManager jobManager = GsonUtils.GSON.fromJson(json, InsertOverwriteJobManager.class);
        return jobManager;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (!GlobalStateMgr.isCheckpointThread()) {
            if (runningJobs == null) {
                runningJobs = Lists.newArrayList();
            }
            for (InsertOverwriteJob job : overwriteJobMap.values()) {
                if (!job.isFinished()) {
                    LOG.info("add insert overwrite job:{} to runningJobs, state:{}",
                            job.getJobId(), job.getJobState());
                    runningJobs.add(job);
                }
            }
        }
    }
}
