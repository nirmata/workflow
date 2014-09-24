package com.nirmata.workflow.details;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagEntryModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagModel;
import com.nirmata.workflow.models.*;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.spi.JsonSerializer;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.spi.TaskExecutionStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.details.InternalJsonSerializer.newDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.fromBytes;
import static com.nirmata.workflow.spi.JsonSerializer.getTaskExecutionResult;
import static com.nirmata.workflow.spi.JsonSerializer.toBytes;

public class Scheduler implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManager workflowManager;
    private final LeaderSelector leaderSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final EnsurePath ensureCompletedRunPath;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public Scheduler(WorkflowManager workflowManager)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
        {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                Scheduler.this.takeLeadership();
            }
        };
        leaderSelector = new LeaderSelector(workflowManager.getCurator(), ZooKeeperConstants.getSchedulerLeaderPath(), listener);
        leaderSelector.autoRequeue();

        ensureCompletedRunPath = workflowManager.getCurator().newNamespaceAwareEnsurePath(ZooKeeperConstants.getCompletedRunParentPath());
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        leaderSelector.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            leaderSelector.close();
        }
    }

    private void checkStartNewRuns(PathChildrenCache runsCache)
    {
        StateCache localStateCache = workflowManager.getStateCache();    // save local value so we're safe if master state cache changes

        for ( ScheduleId scheduleId : localStateCache.getSchedules().keySet() )
        {
            if ( !scheduleIsActive(runsCache, scheduleId) )
            {
                ScheduleModel schedule = localStateCache.getSchedules().get(scheduleId);
                if ( schedule != null )
                {
                    ScheduleExecutionModel scheduleExecution = localStateCache.getScheduleExecutions().get(scheduleId);
                    if ( scheduleExecution == null )
                    {
                        scheduleExecution = new ScheduleExecutionModel(scheduleId, LocalDateTime.now(), LocalDateTime.now(), 0);
                    }
                    if ( schedule.shouldExecuteNow(scheduleExecution) )
                    {
                        startWorkflow(scheduleExecution, schedule, localStateCache);
                    }
                }
                else
                {
                    log.warn("Could not find schedule " + scheduleId);
                }
            }
        }
    }

    private boolean scheduleIsActive(PathChildrenCache runsCache, ScheduleId scheduleId)
    {
        Optional<ChildData> any = runsCache
            .getCurrentData()
            .stream()
            .filter(childData -> getDenormalizedWorkflow(fromBytes(childData.getData())).getScheduleId().equals(scheduleId))
            .findAny();
        return any.isPresent();
    }

    private void startWorkflow(ScheduleExecutionModel scheduleExecution, ScheduleModel schedule, StateCache localStateCache)
    {
        WorkflowModel workflow = localStateCache.getWorkflows().get(schedule.getWorkflowId());
        if ( workflow == null )
        {
            String message = "Expected workflow not found in StateCache. WorkflowId: " + schedule.getWorkflowId();
            log.error(message);
            throw new RuntimeException(message);
        }

        TaskDagModel taskDag = localStateCache.getTaskDagContainers().get(workflow.getTaskDagId());
        if ( taskDag == null )
        {
            String message = "Expected taskDag not found in StateCache. TaskDagId: " + workflow.getTaskDagId();
            log.error(message);
            throw new RuntimeException(message);
        }

        Map<TaskId, TaskModel> tasks = Maps.newHashMap();
        RunnableTaskDagModel runnableTaskDag = new RunnableTaskDagBuilder(taskDag).build();
        for ( RunnableTaskDagEntryModel entry : runnableTaskDag.getEntries() )
        {
            if ( entry.getTaskId().isValid() )
            {
                TaskModel task = localStateCache.getTasks().get(entry.getTaskId());
                if ( task == null )
                {
                    String message = "Expected task not found in StateCache. TaskId: " + entry.getTaskId();
                    log.error(message);
                    throw new RuntimeException(message);
                }
                tasks.put(task.getTaskId(), task);
            }
        }

        DenormalizedWorkflowModel denormalizedWorkflow = new DenormalizedWorkflowModel(new RunId(), WorkflowStatus.RUNNING, scheduleExecution, workflow.getWorkflowId(), tasks, workflow.getName(), runnableTaskDag, LocalDateTime.now(Clock.systemUTC()));
        byte[] json = toJson(log, denormalizedWorkflow);

        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getRunPath(denormalizedWorkflow.getRunId()), json);
            logWorkflowStarted(schedule);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // happens due to cache latency
            log.debug("Workflow already started: " + workflow);
        }
        catch ( Exception e )
        {
            log.error("Could not create workflow node: " + workflow, e);
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected void logWorkflowStarted(ScheduleModel schedule)
    {
        log.info("Started workflow: " + schedule.getWorkflowId());
    }

    private void completeWorkflow(DenormalizedWorkflowModel workflow, WorkflowStatus workflowStatus)
    {
        ScheduleExecutionModel scheduleExecution = workflow.getScheduleExecution();
        ScheduleExecutionModel updatedScheduleExecution = new ScheduleExecutionModel(scheduleExecution.getScheduleId(), workflow.getStartDateUtc(), LocalDateTime.now(Clock.systemUTC()), scheduleExecution.getExecutionQty() + 1);
        workflowManager.getStorageBridge().updateScheduleExecution(updatedScheduleExecution);

        String completedRunPath = ZooKeeperConstants.getCompletedRunPath(workflow.getRunId());
        String runPath = ZooKeeperConstants.getRunPath(workflow.getRunId());
        try
        {
            DenormalizedWorkflowModel completedWorkflow = new DenormalizedWorkflowModel(workflow.getRunId(), workflowStatus, workflow.getScheduleExecution(), workflow.getWorkflowId(), workflow.getTasks(), workflow.getName(), workflow.getRunnableTaskDag(), workflow.getStartDateUtc());
            workflowManager.getCurator().inTransaction()
                    .delete().forPath(runPath)
                .and()
                    .create().forPath(completedRunPath, toJson(log, completedWorkflow))
                .and()
                    .commit();
            log.info("Workflow completed: " + workflow);
        }
        catch ( KeeperException.NodeExistsException e )
        {
            log.debug("Workflow already completed: " + workflow);
        }
        catch ( Exception e )
        {
            log.error("Could not create completed run node: " + workflow, e);
            throw new RuntimeException(e);
        }
    }

    private void cancelWorkflow(PathChildrenCache runsCache, RunId runId)
    {
        DenormalizedWorkflowModel workflow = getDenormalizedWorkflowModel(runsCache, runId);
        if ( workflow != null ) // otherwise, it's cache latency
        {
            completeWorkflow(workflow, WorkflowStatus.FAILED_INTERNAL);
        }
        else
        {
            log.debug("Workflow already canceled: " + runId);
        }
    }

    private void updateTasks(PathChildrenCache runsCache, PathChildrenCache completedTasksCache, PathChildrenCache startedTasksCache, RunId runId)
    {
        DenormalizedWorkflowModel workflow = getDenormalizedWorkflowModel(runsCache, runId);
        if ( workflow == null )
        {
            return; // it must have been canceled
        }

        Set<TaskId> completedTasks = Sets.newHashSet();
        workflow.getRunnableTaskDag().getEntries().forEach(entry -> {
            TaskId taskId = entry.getTaskId();
            boolean taskIsComplete = taskIsComplete(completedTasksCache, runId, taskId);
            if ( taskIsComplete )
            {
                completedTasks.add(taskId);
            }
            if ( taskId.isValid() && !taskIsComplete && !taskIsStarted(startedTasksCache, runId, taskId) )
            {
                boolean allDependenciesAreComplete = entry.getDependencies().stream().allMatch(id -> taskIsComplete(completedTasksCache, runId, id));
                if ( allDependenciesAreComplete )
                {
                    queueTask(runId, workflow.getScheduleId(), workflow.getTasks().get(taskId));
                }
            }
        });

        Set<TaskId> entries = workflow.getRunnableTaskDag().getEntries().stream().map(RunnableTaskDagEntryModel::getTaskId).collect(Collectors.toSet());
        if ( completedTasks.equals(entries))
        {
            completeWorkflow(workflow, WorkflowStatus.COMPLETED);
        }
    }

    private DenormalizedWorkflowModel getDenormalizedWorkflowModel(PathChildrenCache runsCache, RunId runId)
    {
        String path = ZooKeeperConstants.getRunPath(runId);
        ChildData currentData = runsCache.getCurrentData(path);
        return (currentData != null) ? getDenormalizedWorkflow(fromBytes(currentData.getData())) : null;
    }

    private void queueTask(RunId runId, ScheduleId scheduleId, TaskModel task)
    {
        String path = ZooKeeperConstants.getStartedTaskPath(runId, task.getTaskId());
        try
        {
            StartedTaskModel startedTask = new StartedTaskModel(workflowManager.getConfiguration().getInstanceName(), LocalDateTime.now(Clock.systemUTC()));
            byte[] data = JsonSerializer.toBytes(JsonSerializer.newStartedTask(startedTask));
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path, data);
            Queue queue = task.isIdempotent() ? workflowManager.getIdempotentTaskQueue() : workflowManager.getNonIdempotentTaskQueue();
            queue.put(new ExecutableTaskModel(runId, scheduleId, task));
            log.info("Queued task: " + task);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            log.debug("Task already queued: " + task);
            // race due to caching latency - task already started
        }
        catch ( Exception e )
        {
            String message = "Could not start task " + task;
            log.error(message, e);
            throw new RuntimeException(e);
        }
    }

    private boolean taskIsStarted(PathChildrenCache startedTasksCache, RunId runId, TaskId taskId)
    {
        String startedTaskPath = ZooKeeperConstants.getStartedTaskPath(runId, taskId);
        return (startedTasksCache.getCurrentData(startedTaskPath) != null);
    }

    private boolean taskIsComplete(PathChildrenCache completedTasksCache, RunId runId, TaskId taskId)
    {
        if ( !taskId.isValid() )
        {
            return true;
        }
        String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
        return (completedTasksCache.getCurrentData(completedTaskPath) != null);
    }

    static byte[] toJson(Logger log, DenormalizedWorkflowModel denormalizedWorkflow)
    {
        byte[] json = toBytes(newDenormalizedWorkflow(denormalizedWorkflow));
        if ( json.length > ZooKeeperConstants.MAX_PAYLOAD )
        {
            String message = "JSON payload for workflow too big: " + denormalizedWorkflow;
            log.error(message);
            throw new RuntimeException(message);
        }
        return json;
    }

    private void takeLeadership()
    {
        PathChildrenCache completedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTasksParentPath(), true);
        PathChildrenCache startedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getStartedTasksParentPath(), false);
        PathChildrenCache runsCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getRunsParentPath(), true);
        long lastRunCheckMs = 0;
        try
        {
            BlockingQueue<RunIdWithStatus> updatedRunIds = Queues.newLinkedBlockingQueue();
            completedTasksCache.getListenable().addListener((client, event) -> {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    TaskExecutionResult taskExecutionResult = getTaskExecutionResult(fromBytes(event.getData().getData()));
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                    updatedRunIds.add(new RunIdWithStatus(runId, taskExecutionResult.getStatus()));
                    completedTasksCache.clearDataBytes(event.getData().getPath());
                }
            });
            runsCache.getListenable().addListener((client, event) -> {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(event.getData().getPath()));
                    updatedRunIds.add(new RunIdWithStatus(runId, TaskExecutionStatus.SUCCESS));
                }
            });
            completedTasksCache.start();
            runsCache.start();
            startedTasksCache.start();

            while ( !Thread.currentThread().isInterrupted() )
            {
                ensurePaths();

                RunIdWithStatus idWithStatus = updatedRunIds.poll(workflowManager.getConfiguration().getSchedulerSleepMs(), TimeUnit.MILLISECONDS);
                if ( idWithStatus != null )
                {
                    if ( idWithStatus.getStatus() == TaskExecutionStatus.FAILED_STOP )
                    {
                        cancelWorkflow(runsCache, idWithStatus.getRunId());
                    }
                    else
                    {
                        updateTasks(runsCache, completedTasksCache, startedTasksCache, idWithStatus.getRunId());
                    }
                }

                if ( (System.currentTimeMillis() - lastRunCheckMs) >= workflowManager.getConfiguration().getSchedulerSleepMs() )
                {
                    checkStartNewRuns(runsCache);
                    lastRunCheckMs = System.currentTimeMillis();
                }
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Throwable e )
        {
            log.error("Exception while running scheduler", e);
        }
        finally
        {
            CloseableUtils.closeQuietly(completedTasksCache);
            CloseableUtils.closeQuietly(startedTasksCache);
            CloseableUtils.closeQuietly(runsCache);
        }
    }

    private void ensurePaths()
    {
        try
        {
            ensureCompletedRunPath.ensure(workflowManager.getCurator().getZookeeperClient());
        }
        catch ( Exception e )
        {
            String message = "Could not ensure paths";
            log.error(message, e);
        }
    }
}
