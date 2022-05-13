// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
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

    private ExecutorService cancelJobExecutorService;

    private List<InsertOverwriteJob> runningJobs;

    private ReentrantReadWriteLock lock;

    public InsertOverwriteJobManager() {
        this.overwriteJobMap = Maps.newHashMap();
        this.partitionsWithOverwrite = Maps.newHashMap();
        ThreadFactory threadFactory = new DefaultThreadFactory("cancel-thread");
        this.cancelJobExecutorService = Executors.newSingleThreadExecutor(threadFactory);
        this.runningJobs = Lists.newArrayList();
        this.lock = new ReentrantReadWriteLock();
    }

    public InsertOverwriteJob getOverwriteJob(long jobId) {
        lock.readLock().lock();
        try {
            return overwriteJobMap.get(jobId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean submitJob(InsertOverwriteJob job) {
        try {
            registerOverwriteJob(job);
            InsertOverwriteJob.OverwriteJobState state = job.run();
            return state == InsertOverwriteJob.OverwriteJobState.SUCCESS;
        } finally {
            deregisterOverwriteJob(job.getJobId());
        }
    }

    public boolean registerOverwriteJob(InsertOverwriteJob job) {
        lock.writeLock().lock();
        try {
            if (overwriteJobMap.containsKey(job.getJobId())) {
                return false;
            }
            // check whether there is a overwrite job running in partitions
            if (hasRunningOverwriteJob(job.getTargetTableId(), job.getTargetPartitionIds())) {
                return false;
            }
            // 需要区分分区表和非分区表
            /*
            if (partitionsWithOverwrite.containsKey(job.getTargetTableId())) {
                Set<Long> partitionsWithJob = partitionsWithOverwrite.get(job.getTargetTableId());
                if (partitionsWithJob == null) {
                    // it means the table has running insert overwrite job
                    return false;
                }
                List<Long> partitionsToAdd = job.getTargetPartitionIds();
                if (partitionsToAdd.stream().anyMatch(id -> partitionsWithJob.contains(id))) {
                    LOG.warn("table:{} has already running insert overwrite job.", job.getTargetTableName());
                    return false;
                }
            }

             */
            overwriteJobMap.put(job.getJobId(), job);
            Set<Long> partitions = partitionsWithOverwrite.getOrDefault(job.getTargetTableId(), Sets.newHashSet());
            partitions.addAll(job.getTargetPartitionIds());
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
            return true;
        } catch (Exception e) {
            LOG.warn("deregister overwrite job failed", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(Long tableId) {
        lock.readLock().lock();
        try {
            return partitionsWithOverwrite.containsKey(tableId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(Long tableId, Set<Long> partitions) {
        lock.readLock().lock();
        try {
            boolean tableExist = partitionsWithOverwrite.containsKey(tableId);
            if (!tableExist) {
                return false;
            }
            if (partitions == null) {
                // means check table
                return true;
            }
            Set<Long> runningPartitions = partitionsWithOverwrite.get(tableId);
            if (runningPartitions == null) {
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
        if (!Catalog.isCheckpointThread()) {
            cancelJobExecutorService.submit(() -> {
                // wait until serving catalog is ready
                while (!Catalog.getServingCatalog().isReady()) {
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
                            if (job.getJobState() == InsertOverwriteJob.OverwriteJobState.COMMITTING) {
                                // if the state is COMMITTING, then it means the creation of temp partitions
                                // and the insert are successful. So treat this state as success.
                                // just recommit the insert overwrite
                                job.handle();
                            } else {
                                job.cancel();
                            }
                        } finally {
                            deregisterOverwriteJob(job.getJobId());
                        }
                    }
                    runningJobs = null;
                }
            });
        }
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
        if (!Catalog.isCheckpointThread()) {
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
