// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Maps;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import jersey.repackaged.com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// 是否需要持久化？
public class InsertOverwriteJobManager {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobManager.class);

    private Map<Long, InsertOverwriteJob> overwriteJobMap;

    // tableId -> partitionId list
    private Map<Long, Set<Long>> partitionsWithOverwrite;

    private ExecutorService overwriteJobExecutorService;

    private ReentrantReadWriteLock lock;

    public InsertOverwriteJobManager() {
        this.overwriteJobMap = Maps.newHashMap();
        this.partitionsWithOverwrite = Maps.newHashMap();
        // TODO: add named and limited thread pool
        this.overwriteJobExecutorService = Executors.newCachedThreadPool();
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
        /*
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            try {
                registerOverwriteJob(job);
                InsertOverwriteJob.OverwriteJobState state = job.run();
                return state == InsertOverwriteJob.OverwriteJobState.SUCCESS;
            } finally {
                deregisterOverwriteJob(job.getJobId());
            }

        }, overwriteJobExecutorService);
        return future;

         */
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

    public boolean hasRunningOverwriteJob(Long tableId, List<Long> partitions) {
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
                    jobInfo.getDbId(), jobInfo.getTableId(), jobInfo.getOriginInsertSql());
            boolean registered = registerOverwriteJob(insertOverwriteJob);
            if (!registered) {
                LOG.warn("register insert overwrite job failed. jobId:{}", insertOverwriteJob.getJobId());
            }
        } catch (Exception e) {
            LOG.warn("replay insert overwrite job failed. jobId:{}", jobInfo.getJobId(), e);
        }
    }

    public void replayInsertOverwriteStateChange(InsertOverwriteStateChangeInfo info) {
        InsertOverwriteJob job = getOverwriteJob(info.getJobId());
        job.replayStateChange(info);
    }
}
