// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertOverwriteJobManager {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobManager.class);

    private Map<Long, InsertOverwriteJob> overwriteJobMap;

    // tableId -> partitionId list
    private Map<Long, List<Long>> partitionsWithOverwrite;

    private ExecutorService overwriteJobExecutorService;

    private ReentrantReadWriteLock lock;

    public InsertOverwriteJobManager() {
        this.overwriteJobMap = Maps.newHashMap();
        this.partitionsWithOverwrite = Maps.newHashMap();
        // TODO: add named and limited thread pool
        this.overwriteJobExecutorService = Executors.newCachedThreadPool();
    }

    public InsertOverwriteJob getOverwriteJob(long jobId) {
        lock.readLock().lock();
        try {
            return overwriteJobMap.get(jobId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Future submitJob(InsertOverwriteJob job) {
        CompletableFuture future = CompletableFuture.runAsync(() -> {
            try {
                registerOverwriteJob(job);
                job.run();
            } finally {
                deregisterOverwriteJob(job.getJobId());
            }

        }, overwriteJobExecutorService);
        return future;
    }

    public boolean registerOverwriteJob(InsertOverwriteJob job) {
        lock.writeLock().lock();
        try {
            if (overwriteJobMap.containsKey(job.getJobId())) {
                return false;
            }
            if (partitionsWithOverwrite.containsKey(job.getTargetTableName())) {
                List<Long> partitionsWithJob = partitionsWithOverwrite.get(job.getTargetTableName());
                List<Long> partitionsToAdd = job.getTargetPartitionIds();
                if (partitionsToAdd.stream().anyMatch(id -> partitionsWithJob.contains(id))) {
                    LOG.warn("table:{} has already running insert overwrite job.", job.getTargetTableName());
                    return false;
                }
            }
            overwriteJobMap.put(job.getJobId(), job);
            List<Long> partitions = partitionsWithOverwrite.getOrDefault(job.getTargetTableName(), Lists.newArrayList());
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
        lock.writeLock().lock();
        try {
            if (!overwriteJobMap.containsKey(jobid)) {
                return true;
            }
            InsertOverwriteJob job = overwriteJobMap.get(jobid);
            List<Long> partitionIds = partitionsWithOverwrite.get(job.getTargetTableName());
            partitionIds.removeAll(job.getTargetPartitionIds());
            if (partitionIds.isEmpty()) {
                partitionsWithOverwrite.remove(job.getTargetTableName());
            }
            return true;
        } catch (Exception e) {
            LOG.warn("deregister overwrite job failed", e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(String tableName) {
        lock.readLock().lock();
        try {
            return partitionsWithOverwrite.containsKey(tableName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(String tableName, List<Long> partitions) {
        lock.readLock().lock();
        try {
            boolean tableExist = partitionsWithOverwrite.containsKey(tableName);
            if (!tableExist) {
                return false;
            }
            if (partitions == null) {
                // means check table
                return true;
            }
            List<Long> runningPartitions = partitionsWithOverwrite.get(tableName);
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
            if (registered) {
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
