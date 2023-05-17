// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.meta.LimitExceededException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.optimizer.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.SubmitResult.SubmitStatus.SUBMITTED;

public class TaskManager {

    private static final Logger LOG = LogManager.getLogger(TaskManager.class);

    // taskId -> Task , Task may have Normal Task, Periodical Task
    // every TaskRun must be generated by a Task
    private final Map<Long, Task> idToTaskMap;
    // taskName -> Task, include Manual Task, Periodical Task
    private final Map<String, Task> nameToTaskMap;
    private final Map<Long, ScheduledFuture<?>> periodFutureMap;

    // include PENDING/RUNNING taskRun;
    private final TaskRunManager taskRunManager;

    // The periodScheduler is used to generate the corresponding TaskRun on time for the Periodical Task.
    // This scheduler can use the time wheel to optimize later.
    private final ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);

    // The dispatchTaskScheduler is responsible for periodically checking whether the running TaskRun is completed
    // and updating the status. It is also responsible for placing pending TaskRun in the running TaskRun queue.
    // This operation need to consider concurrency.
    // This scheduler can use notify/wait to optimize later.
    private final ScheduledExecutorService dispatchScheduler = Executors.newScheduledThreadPool(1);

    // Use to concurrency control
    private final QueryableReentrantLock taskLock;

    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public TaskManager() {
        idToTaskMap = Maps.newConcurrentMap();
        nameToTaskMap = Maps.newConcurrentMap();
        periodFutureMap = Maps.newConcurrentMap();
        taskRunManager = new TaskRunManager();
        taskLock = new QueryableReentrantLock(true);
    }

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            clearUnfinishedTaskRun();
            registerPeriodicalTask();
            dispatchScheduler.scheduleAtFixedRate(() -> {
                if (!taskRunManager.tryTaskRunLock()) {
                    return;
                }
                try {
                    taskRunManager.checkRunningTaskRun();
                    taskRunManager.scheduledPendingTaskRun();
                } catch (Exception ex) {
                    LOG.warn("failed to dispatch task.", ex);
                } finally {
                    taskRunManager.taskRunUnlock();
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
    }

    private void registerPeriodicalTask() {
        for (Task task : nameToTaskMap.values()) {
            if (task.getType() != Constants.TaskType.PERIODICAL) {
                continue;
            }

            TaskSchedule taskSchedule = task.getSchedule();
            if (task.getState() != Constants.TaskState.ACTIVE) {
                continue;
            }

            if (taskSchedule == null) {
                continue;
            }
            long period = TimeUtils.convertTimeUnitValueToSecond(taskSchedule.getPeriod(),
                    taskSchedule.getTimeUnit());
            LocalDateTime startTime = Utils.getDatetimeFromLong(taskSchedule.getStartTime());
            LocalDateTime scheduleTime = LocalDateTime.now();
            long initialDelay = getInitialDelayTime(period, startTime, scheduleTime);
            // Tasks that run automatically have the lowest priority,
            // but are automatically merged if they are found to be merge-able.
            ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(),
                    true, task.getProperties());
            ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() ->
                            executeTask(task.getName(), option), initialDelay,
                    period, TimeUnit.SECONDS);
            periodFutureMap.put(task.getId(), future);
        }
    }

    @VisibleForTesting
    static long getInitialDelayTime(long period, LocalDateTime startTime,
                                    LocalDateTime scheduleTime) {
        Duration duration = Duration.between(scheduleTime, startTime);
        long initialDelay = duration.getSeconds();
        // if startTime < now, start scheduling from the next period
        if (initialDelay < 0) {
            return ((initialDelay % period) + period) % period;
        } else {
            return initialDelay;
        }
    }

    private void clearUnfinishedTaskRun() {
        if (!taskRunManager.tryTaskRunLock()) {
            return;
        }
        try {
            Iterator<Long> pendingIter = taskRunManager.getPendingTaskRunMap().keySet().iterator();
            while (pendingIter.hasNext()) {
                Queue<TaskRun> taskRuns = taskRunManager.getPendingTaskRunMap().get(pendingIter.next());
                while (!taskRuns.isEmpty()) {
                    TaskRun taskRun = taskRuns.poll();
                    taskRun.getStatus().setErrorMessage("Fe abort the task");
                    taskRun.getStatus().setErrorCode(-1);
                    taskRun.getStatus().setState(Constants.TaskRunState.FAILED);
                    taskRunManager.getTaskRunHistory().addHistory(taskRun.getStatus());
                    TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun.getTaskId(), taskRun.getStatus(),
                            Constants.TaskRunState.PENDING, Constants.TaskRunState.FAILED);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                }
                pendingIter.remove();
            }
            Iterator<Long> runningIter = taskRunManager.getRunningTaskRunMap().keySet().iterator();
            while (runningIter.hasNext()) {
                TaskRun taskRun = taskRunManager.getRunningTaskRunMap().get(runningIter.next());
                taskRun.getStatus().setErrorMessage("Fe abort the task");
                taskRun.getStatus().setErrorCode(-1);
                taskRun.getStatus().setState(Constants.TaskRunState.FAILED);
                taskRun.getStatus().setFinishTime(System.currentTimeMillis());
                runningIter.remove();
                taskRunManager.getTaskRunHistory().addHistory(taskRun.getStatus());
                TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun.getTaskId(), taskRun.getStatus(),
                        Constants.TaskRunState.RUNNING, Constants.TaskRunState.FAILED);
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        } finally {
            taskRunManager.taskRunUnlock();
        }
    }

    public void createTask(Task task, boolean isReplay) throws DdlException {
        if (!tryTaskLock()) {
            throw new DdlException("Failed to get task lock when create Task [" + task.getName() + "]");
        }
        try {
            if (nameToTaskMap.containsKey(task.getName())) {
                throw new DdlException("Task [" + task.getName() + "] already exists");
            }
            if (!isReplay) {
                // TaskId should be assigned by the framework
                Preconditions.checkArgument(task.getId() == 0);
                task.setId(GlobalStateMgr.getCurrentState().getNextId());
            }
            if (task.getType() == Constants.TaskType.PERIODICAL) {
                task.setState(Constants.TaskState.ACTIVE);
                if (!isReplay) {
                    TaskSchedule schedule = task.getSchedule();
                    if (schedule == null) {
                        throw new DdlException("Task [" + task.getName() + "] has no scheduling information");
                    }
                    LocalDateTime startTime = Utils.getDatetimeFromLong(schedule.getStartTime());
                    Duration duration = Duration.between(LocalDateTime.now(), startTime);
                    long initialDelay = duration.getSeconds();
                    // if startTime < now, start scheduling now
                    if (initialDelay < 0) {
                        initialDelay = 0;
                    }
                    // this operation should only run in master
                    ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() ->
                                    executeTask(task.getName()), initialDelay,
                            TimeUtils.convertTimeUnitValueToSecond(schedule.getPeriod(), schedule.getTimeUnit()),
                            TimeUnit.SECONDS);
                    periodFutureMap.put(task.getId(), future);
                }
            }
            nameToTaskMap.put(task.getName(), task);
            idToTaskMap.put(task.getId(), task);
            if (!isReplay) {
                GlobalStateMgr.getCurrentState().getEditLog().logCreateTask(task);
            }
        } finally {
            taskUnlock();
        }
    }

    private boolean stopScheduler(String taskName) {
        Task task = nameToTaskMap.get(taskName);
        if (task.getType() != Constants.TaskType.PERIODICAL) {
            return false;
        }
        if (task.getState() == Constants.TaskState.PAUSE) {
            return true;
        }
        TaskSchedule taskSchedule = task.getSchedule();
        // this will not happen
        if (taskSchedule == null) {
            LOG.warn("fail to obtain scheduled info for task [{}]", task.getName());
            return true;
        }
        ScheduledFuture<?> future = periodFutureMap.get(task.getId());
        if (future == null) {
            LOG.warn("fail to obtain scheduled info for task [{}]", task.getName());
            return true;
        }
        boolean isCancel = future.cancel(true);
        if (!isCancel) {
            LOG.warn("fail to cancel scheduler for task [{}]", task.getName());
        }
        return isCancel;
    }

    public boolean killTask(String taskName, boolean clearPending) {
        Task task = nameToTaskMap.get(taskName);
        if (task == null) {
            return false;
        }
        if (clearPending) {
            if (!taskRunManager.tryTaskRunLock()) {
                return false;
            }
            try {
                taskRunManager.getPendingTaskRunMap().remove(task.getId());
            } catch (Exception ex) {
                LOG.warn("failed to kill task.", ex);
            } finally {
                taskRunManager.taskRunUnlock();
            }
        }
        return taskRunManager.killTaskRun(task.getId());
    }

    public SubmitResult executeTask(String taskName) {
        return executeTask(taskName, new ExecuteOption());
    }

    public SubmitResult executeTask(String taskName, ExecuteOption option) {
        Task task = getTask(taskName);
        if (task == null) {
            return new SubmitResult(null, SubmitResult.SubmitStatus.FAILED);
        }
        if (option.getIsSync()) {
            return executeTaskSync(task, option);
        } else {
            return executeTaskAsync(task, option);
        }
    }

    // for test
    public SubmitResult executeTaskSync(Task task) {
        return executeTaskSync(task, new ExecuteOption());
    }

    public SubmitResult executeTaskSync(Task task, ExecuteOption option) {
        TaskRun taskRun;
        SubmitResult submitResult;
        if (!tryTaskLock()) {
            throw new DmlException("Failed to get task lock when execute Task sync[" + task.getName() + "]");
        }
        try {
            taskRun = TaskRunBuilder.newBuilder(task).setConnectContext(ConnectContext.get()).build();
            submitResult = taskRunManager.submitTaskRun(taskRun, option);
            if (submitResult.getStatus() != SUBMITTED) {
                throw new DmlException("execute task:" + task.getName() + " failed");
            }
        } finally {
            taskUnlock();
        }
        try {
            Constants.TaskRunState taskRunState = taskRun.getFuture().get();
            if (taskRunState != Constants.TaskRunState.SUCCESS) {
                throw new DmlException("execute task: %s failed. task source:%s, task run state:%s",
                        task.getName(), task.getSource(), taskRunState);
            }
            return submitResult;
        } catch (Exception e) {
            throw new DmlException("execute task: %s failed.", e, task.getName());
        }
    }

    public SubmitResult executeTaskAsync(Task task, ExecuteOption option) {
        return taskRunManager
                .submitTaskRun(TaskRunBuilder.newBuilder(task).properties(option.getTaskRunProperties()).type(option).
                        build(), option);
    }

    public void dropTasks(List<Long> taskIdList, boolean isReplay) {
        // keep nameToTaskMap and manualTaskMap consist
        if (!tryTaskLock()) {
            return;
        }
        try {
            for (long taskId : taskIdList) {
                Task task = idToTaskMap.get(taskId);
                if (task == null) {
                    LOG.warn("drop taskId {} failed because task is null", taskId);
                    continue;
                }
                if (task.getType() == Constants.TaskType.PERIODICAL && !isReplay) {
                    boolean isCancel = stopScheduler(task.getName());
                    if (!isCancel) {
                        continue;
                    }
                    periodFutureMap.remove(task.getId());
                }
                if (!killTask(task.getName(), true)) {
                    LOG.error("kill task failed: " + task.getName());
                }
                idToTaskMap.remove(task.getId());
                nameToTaskMap.remove(task.getName());
            }

            if (!isReplay) {
                GlobalStateMgr.getCurrentState().getEditLog().logDropTasks(taskIdList);
            }
        } finally {
            taskUnlock();
        }
        LOG.info("drop tasks:{}", taskIdList);
    }

    public List<Task> showTasks(String dbName) {
        List<Task> taskList = Lists.newArrayList();
        if (dbName == null) {
            taskList.addAll(nameToTaskMap.values());
        } else {
            for (Map.Entry<String, Task> entry : nameToTaskMap.entrySet()) {
                Task task = entry.getValue();

                if (task.getDbName() != null && task.getDbName().equals(dbName)) {
                    taskList.add(task);
                }
            }
        }
        return taskList;
    }

    private boolean tryTaskLock() {
        try {
            if (!taskLock.tryLock(5, TimeUnit.SECONDS)) {
                Thread owner = taskLock.getOwner();
                if (owner != null) {
                    LOG.warn("task lock is held by: {}", Util.dumpThread(owner, 50));
                } else {
                    LOG.warn("task lock owner is null");
                }
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task lock", e);
        }
        return false;
    }

    public void taskUnlock() {
        this.taskLock.unlock();
    }

    public void replayCreateTask(Task task) {
        if (task.getType() == Constants.TaskType.PERIODICAL) {
            TaskSchedule taskSchedule = task.getSchedule();
            if (taskSchedule == null) {
                LOG.warn("replay a null schedule period Task [{}]", task.getName());
                return;
            }
        }
        if (task.getExpireTime() > 0 && System.currentTimeMillis() > task.getExpireTime()) {
            return;
        }
        try {
            createTask(task, true);
        } catch (DdlException e) {
            LOG.warn("failed to replay create task [{}]", task.getName(), e);
        }
    }

    public void replayDropTasks(List<Long> taskIdList) {
        dropTasks(taskIdList, true);
    }

    public TaskRunManager getTaskRunManager() {
        return taskRunManager;
    }

    public ShowResultSet handleSubmitTaskStmt(SubmitTaskStmt submitTaskStmt) throws DdlException {
        Task task = TaskBuilder.buildTask(submitTaskStmt, ConnectContext.get());
        String taskName = task.getName();
        SubmitResult submitResult;
        try {
            createTask(task, false);
            submitResult = executeTask(taskName);
        } catch (DdlException ex) {
            if (ex.getMessage().contains("Failed to get task lock")) {
                submitResult = new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
            } else {
                LOG.warn("Failed to create Task [{}]" + taskName, ex);
                throw ex;
            }
        }

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("TaskName", ScalarType.createVarchar(40)));
        builder.addColumn(new Column("Status", ScalarType.createVarchar(10)));
        List<String> item = ImmutableList.of(taskName, submitResult.getStatus().toString());
        List<List<String>> result = ImmutableList.of(item);
        return new ShowResultSet(builder.build(), result);
    }

    public long loadTasks(DataInputStream dis, long checksum) throws IOException {
        int taskCount = 0;
        try {
            String s = Text.readString(dis);
            SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
            if (data != null) {
                if (data.tasks != null) {
                    for (Task task : data.tasks) {
                        replayCreateTask(task);
                    }
                    taskCount = data.tasks.size();
                }

                if (data.runStatus != null) {
                    for (TaskRunStatus runStatus : data.runStatus) {
                        replayCreateTaskRun(runStatus);
                    }
                }
            }
            checksum ^= taskCount;
            LOG.info("finished replaying TaskManager from image");
        } catch (EOFException e) {
            LOG.info("no TaskManager to replay.");
        }
        return checksum;
    }

    public long saveTasks(DataOutputStream dos, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.tasks = new ArrayList<>(nameToTaskMap.values());
        checksum ^= data.tasks.size();
        data.runStatus = showTaskRunStatus(null);
        String s = GsonUtils.GSON.toJson(data);
        boolean retry = false;
        try {
            Text.writeString(dos, s);
        } catch (LimitExceededException ex) {
            retry = true;
        }
        if (retry) {
            int beforeLength = s.length();
            taskRunManager.getTaskRunHistory().forceGC();
            data.runStatus = showTaskRunStatus(null);
            s = GsonUtils.GSON.toJson(data);
            LOG.warn("Too much task metadata triggers forced task_run GC, " +
                    "length before GC:{}, length after GC:{}.", beforeLength, s.length());
            Text.writeString(dos, s);
        }
        return checksum;
    }

    public List<TaskRunStatus> showTaskRunStatus(String dbName) {
        List<TaskRunStatus> taskRunList = Lists.newArrayList();
        if (dbName == null) {
            for (Queue<TaskRun> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                taskRunList.addAll(pTaskRunQueue.stream().map(TaskRun::getStatus).collect(Collectors.toList()));
            }
            taskRunList.addAll(taskRunManager.getRunningTaskRunMap().values().stream().map(TaskRun::getStatus)
                    .collect(Collectors.toList()));
            taskRunList.addAll(taskRunManager.getTaskRunHistory().getAllHistory());
        } else {
            for (Queue<TaskRun> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                taskRunList.addAll(pTaskRunQueue.stream().map(TaskRun::getStatus)
                        .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            }
            taskRunList.addAll(taskRunManager.getRunningTaskRunMap().values().stream().map(TaskRun::getStatus)
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));
            taskRunList.addAll(taskRunManager.getTaskRunHistory().getAllHistory().stream()
                    .filter(u -> u.getDbName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskRunList;
    }

    /**
     * Return the last refresh TaskRunStatus for the task which the source type is MV.
     * The iteration order is by the task refresh time:
     *      PendingTaskRunMap > RunningTaskRunMap > TaskRunHistory
     * TODO: Maybe only return needed MVs rather than all MVs.
     */
    public Map<String, TaskRunStatus> showMVLastRefreshTaskRunStatus(String dbName) {
        Map<String, TaskRunStatus> mvNameRunStatusMap = Maps.newHashMap();
        if (dbName == null) {
            for (Queue<TaskRun> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                pTaskRunQueue.stream()
                        .filter(task -> task.getTask().getSource() == Constants.TaskSource.MV)
                        .map(TaskRun::getStatus)
                        .filter(task -> task != null)
                        .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
            }
            taskRunManager.getRunningTaskRunMap().values().stream()
                    .filter(task -> task.getTask().getSource() == Constants.TaskSource.MV)
                    .map(TaskRun::getStatus)
                    .filter(task -> task != null)
                    .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
            taskRunManager.getTaskRunHistory().getAllHistory().stream()
                    .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
        } else {
            for (Queue<TaskRun> pTaskRunQueue : taskRunManager.getPendingTaskRunMap().values()) {
                pTaskRunQueue.stream()
                        .filter(task -> task.getTask().getSource() == Constants.TaskSource.MV)
                        .map(TaskRun::getStatus)
                        .filter(task -> task != null)
                        .filter(u -> u != null && u.getDbName().equals(dbName))
                        .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
            }
            taskRunManager.getRunningTaskRunMap().values().stream()
                    .filter(task -> task.getTask().getSource() == Constants.TaskSource.MV)
                    .map(TaskRun::getStatus)
                    .filter(u -> u != null && u.getDbName().equals(dbName))
                    .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
            taskRunManager.getTaskRunHistory().getAllHistory().stream()
                    .filter(u -> u.getDbName().equals(dbName))
                    .forEach(task -> mvNameRunStatusMap.putIfAbsent(task.getTaskName(), task));
        }
        return mvNameRunStatusMap;
    }

    public void replayCreateTaskRun(TaskRunStatus status) {

        if (status.getState() == Constants.TaskRunState.SUCCESS ||
                status.getState() == Constants.TaskRunState.FAILED) {
            if (System.currentTimeMillis() > status.getExpireTime()) {
                return;
            }
        }
        LOG.info("replayCreateTaskRun:" + status);

        switch (status.getState()) {
            case PENDING:
                String taskName = status.getTaskName();
                Task task = nameToTaskMap.get(taskName);
                if (task == null) {
                    LOG.warn("fail to obtain task name {} because task is null", taskName);
                    return;
                }
                TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                taskRun.initStatus(status.getQueryId(), status.getCreateTime());
                taskRunManager.arrangeTaskRun(taskRun, status.isMergeRedundant());
                break;
            // this will happen in build image
            case RUNNING:
                status.setState(Constants.TaskRunState.FAILED);
                taskRunManager.getTaskRunHistory().addHistory(status);
                break;
            case FAILED:
                taskRunManager.getTaskRunHistory().addHistory(status);
                break;
            case SUCCESS:
                status.setProgress(100);
                taskRunManager.getTaskRunHistory().addHistory(status);
                break;
        }
    }

    public void replayUpdateTaskRun(TaskRunStatusChange statusChange) {
        Constants.TaskRunState fromStatus = statusChange.getFromStatus();
        Constants.TaskRunState toStatus = statusChange.getToStatus();
        Long taskId = statusChange.getTaskId();
        LOG.info("replayUpdateTaskRun:" + statusChange);
        if (fromStatus == Constants.TaskRunState.PENDING) {
            Queue<TaskRun> taskRunQueue = taskRunManager.getPendingTaskRunMap().get(taskId);
            if (taskRunQueue == null) {
                return;
            }
            if (taskRunQueue.size() == 0) {
                taskRunManager.getPendingTaskRunMap().remove(taskId);
                return;
            }

            // It is possible to update out of order for priority queue.
            TaskRun pendingTaskRun = null;
            List<TaskRun> tempQueue = Lists.newArrayList();
            while (!taskRunQueue.isEmpty()) {
                TaskRun taskRun = taskRunQueue.poll();
                if (taskRun.getStatus().getQueryId().equals(statusChange.getQueryId())) {
                    pendingTaskRun = taskRun;
                    break;
                } else {
                    tempQueue.add(taskRun);
                }
            }
            taskRunQueue.addAll(tempQueue);

            if (pendingTaskRun == null) {
                LOG.warn("could not find query_id:{}, taskId:{}, when replay update pendingTaskRun",
                        statusChange.getQueryId(), taskId);
                return;
            }
            TaskRunStatus status = pendingTaskRun.getStatus();

            if (toStatus == Constants.TaskRunState.RUNNING) {
                if (status.getQueryId().equals(statusChange.getQueryId())) {
                    status.setState(Constants.TaskRunState.RUNNING);
                    taskRunManager.getRunningTaskRunMap().put(taskId, pendingTaskRun);
                }
                // for fe restart, should keep logic same as clearUnfinishedTaskRun
            } else if (toStatus == Constants.TaskRunState.FAILED) {
                status.setErrorMessage(statusChange.getErrorMessage());
                status.setErrorCode(statusChange.getErrorCode());
                status.setState(Constants.TaskRunState.FAILED);
                taskRunManager.getTaskRunHistory().addHistory(status);
            }
            if (taskRunQueue.size() == 0) {
                taskRunManager.getPendingTaskRunMap().remove(taskId);
            }
        } else if (fromStatus == Constants.TaskRunState.RUNNING &&
                (toStatus == Constants.TaskRunState.SUCCESS || toStatus == Constants.TaskRunState.FAILED)) {
            // NOTE: TaskRuns before the fe restart will be replayed in `replayCreateTaskRun` which
            // will not be rerun because `InsertOverwriteJobRunner.replayStateChange` will replay, so
            // the taskRun's may be PENDING/RUNNING/SUCCESS.
            TaskRun runningTaskRun = taskRunManager.getRunningTaskRunMap().remove(taskId);
            if (runningTaskRun != null) {
                TaskRunStatus status = runningTaskRun.getStatus();
                if (status.getQueryId().equals(statusChange.getQueryId())) {
                    if (toStatus == Constants.TaskRunState.FAILED) {
                        status.setErrorMessage(statusChange.getErrorMessage());
                        status.setErrorCode(statusChange.getErrorCode());
                    }
                    status.setState(toStatus);
                    status.setProgress(100);
                    status.setFinishTime(statusChange.getFinishTime());
                    status.setExtraMessage(statusChange.getExtraMessage());
                    taskRunManager.getTaskRunHistory().addHistory(status);
                }
            } else {
                // Find the task status from history map.
                String queryId = statusChange.getQueryId();
                TaskRunStatus status = taskRunManager.getTaskRunHistory().getTask(queryId);
                if (status == null) {
                    return;
                }
                // Do update extra message from change status.
                status.setExtraMessage(statusChange.getExtraMessage());
            }
        } else {
            LOG.warn("Illegal TaskRun queryId:{} status transform from {} to {}",
                    statusChange.getQueryId(), fromStatus, toStatus);
        }
    }

    public void replayDropTaskRuns(List<String> queryIdList) {
        Map<String, String> index = Maps.newHashMapWithExpectedSize(queryIdList.size());
        for (String queryId : queryIdList) {
            index.put(queryId, null);
        }
        taskRunManager.getTaskRunHistory().getAllHistory()
                .removeIf(runStatus -> index.containsKey(runStatus.getQueryId()));
    }

    public void replayAlterRunningTaskRunProgress(Map<Long, Integer> taskRunProgresMap) {
        Map<Long, TaskRun> runningTaskRunMap = taskRunManager.getRunningTaskRunMap();
        for (Map.Entry<Long, Integer> entry : taskRunProgresMap.entrySet()) {
            // When replaying the log, the task run may have ended
            // and the status has changed to success or failed
            if (runningTaskRunMap.containsKey(entry.getKey())) {
                runningTaskRunMap.get(entry.getKey()).getStatus().setProgress(entry.getValue());
            }
        }
    }

    public void removeExpiredTasks() {
        long currentTimeMs = System.currentTimeMillis();

        List<Long> taskIdToDelete = Lists.newArrayList();
        if (!tryTaskLock()) {
            return;
        }
        try {
            List<Task> currentTask = showTasks(null);
            for (Task task : currentTask) {
                if (task.getType() == Constants.TaskType.PERIODICAL) {
                    TaskSchedule taskSchedule = task.getSchedule();
                    if (taskSchedule == null) {
                        taskIdToDelete.add(task.getId());
                        LOG.warn("clean up a null schedule periodical Task [{}]", task.getName());
                        continue;
                    }
                    // active periodical task should not clean
                    if (task.getState() == Constants.TaskState.ACTIVE) {
                        continue;
                    }
                }
                Long expireTime = task.getExpireTime();
                if (expireTime > 0 && currentTimeMs > expireTime) {
                    taskIdToDelete.add(task.getId());
                }
            }
        } finally {
            taskUnlock();
        }
        // this will do in checkpoint thread and does not need write log
        dropTasks(taskIdToDelete, true);
    }

    public void removeExpiredTaskRuns() {
        long currentTimeMs = System.currentTimeMillis();

        List<String> historyToDelete = Lists.newArrayList();

        if (!taskRunManager.tryTaskRunLock()) {
            return;
        }
        try {
            // only SUCCESS and FAILED in taskRunHistory
            List<TaskRunStatus> taskRunHistory = taskRunManager.getTaskRunHistory().getAllHistory();
            Iterator<TaskRunStatus> iterator = taskRunHistory.iterator();
            while (iterator.hasNext()) {
                TaskRunStatus taskRunStatus = iterator.next();
                long expireTime = taskRunStatus.getExpireTime();
                if (currentTimeMs > expireTime) {
                    historyToDelete.add(taskRunStatus.getQueryId());
                    taskRunManager.getTaskRunHistory().removeTask(taskRunStatus.getQueryId());
                    iterator.remove();
                }
            }
        } finally {
            taskRunManager.taskRunUnlock();
        }
        LOG.info("remove run history:{}", historyToDelete);
    }

    private static class SerializeData {
        @SerializedName("tasks")
        public List<Task> tasks;

        @SerializedName("runStatus")
        public List<TaskRunStatus> runStatus;
    }

    public boolean containTask(String taskName) {
        if (!tryTaskLock()) {
            throw new DmlException("Failed to get task lock when check Task [" + taskName + "]");
        }
        try {
            return nameToTaskMap.containsKey(taskName);
        } finally {
            taskUnlock();
        }
    }

    public Task getTask(String taskName) {
        if (!tryTaskLock()) {
            throw new DmlException("Failed to get task lock when get Task [" + taskName + "]");
        }
        try {
            return nameToTaskMap.get(taskName);
        } finally {
            taskUnlock();
        }
    }
}
