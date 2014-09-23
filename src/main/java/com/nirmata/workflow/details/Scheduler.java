package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagEntryModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagModel;
import com.nirmata.workflow.models.*;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.details.InternalJsonSerializer.newDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.fromBytes;
import static com.nirmata.workflow.spi.JsonSerializer.toBytes;

public class Scheduler implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManager workflowManager;
    private final LeaderSelector leaderSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

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

    private void checkStartNewRuns(PathChildrenCache runsCache, PathChildrenCache completedTasksCache)
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
                        startWorkflow(runsCache, completedTasksCache, scheduleExecution, schedule, localStateCache);
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

    private void startWorkflow(PathChildrenCache runsCache, PathChildrenCache completedTasksCache, ScheduleExecutionModel scheduleExecution, ScheduleModel schedule, StateCache localStateCache)
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
            TaskModel task = localStateCache.getTasks().get(entry.getTaskId());
            if ( task == null )
            {
                String message = "Expected task not found in StateCache. TaskId: " + entry.getTaskId();
                log.error(message);
                throw new RuntimeException(message);
            }
            tasks.put(task.getTaskId(), task);
        }

        DenormalizedWorkflowModel denormalizedWorkflow = new DenormalizedWorkflowModel(new RunId(), scheduleExecution, workflow.getWorkflowId(), tasks, workflow.getName(), runnableTaskDag, LocalDateTime.now(Clock.systemUTC()));
        byte[] json = toJson(log, denormalizedWorkflow);

        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(ZooKeeperConstants.getRunPath(denormalizedWorkflow.getRunId()), json);
            log.info("Started workflow: " + schedule.getWorkflowId());
            workflowManager.notifyScheduleStarted(schedule.getScheduleId());

            updateTasks(runsCache, completedTasksCache, denormalizedWorkflow.getRunId());
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

    private void updateTasks(PathChildrenCache runsCache, PathChildrenCache completedTasksCache, RunId runId)
    {
        String path = ZooKeeperConstants.getRunPath(runId);
        ChildData currentData = runsCache.getCurrentData(path);
        if ( currentData == null )
        {
            String message = "Notified run not found in cache: " + runId;
            log.warn(message);
            throw new RuntimeException(message);
        }

        DenormalizedWorkflowModel workflow = getDenormalizedWorkflow(fromBytes(currentData.getData()));
        workflow.getRunnableTaskDag().getEntries().forEach(entry -> {
            TaskId taskId = entry.getTaskId();
            if ( taskId.isValid() && !taskIsComplete(completedTasksCache, runId, taskId) )
            {
                boolean allDependenciesAreComplete = entry.getDependencies().stream().allMatch(id -> taskIsComplete(completedTasksCache, runId, id));
                if ( allDependenciesAreComplete )
                {
                    queueTask(runId, workflow.getScheduleId(), workflow.getTasks().get(taskId));
                }
            }
        });
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

    private boolean taskIsComplete(PathChildrenCache completedTasksCache, RunId runId, TaskId taskId)
    {
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
        PathChildrenCache completedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTasksParentPath(), false);
        PathChildrenCache runsCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getRunsParentPath(), true);
        long lastRunCheck = 0;
        try
        {
            BlockingQueue<RunId> updatedRunIds = Queues.newLinkedBlockingQueue();
            completedTasksCache.getListenable().addListener((client, event) -> {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                    updatedRunIds.add(runId);
                }
            });
            completedTasksCache.start();
            runsCache.start();

            while ( !Thread.currentThread().isInterrupted() )
            {
                RunId runId = updatedRunIds.poll(workflowManager.getConfiguration().getSchedulerSleepMs(), TimeUnit.MILLISECONDS);
                updateTasks(runsCache, completedTasksCache, runId);

                if ( (System.currentTimeMillis() - lastRunCheck) >= workflowManager.getConfiguration().getSchedulerSleepMs() )
                {
                    checkStartNewRuns(runsCache, completedTasksCache);
                    lastRunCheck = System.currentTimeMillis();
                }
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Exception e )
        {
            log.error("Exception while running scheduler", e);
        }
        finally
        {
            CloseableUtils.closeQuietly(completedTasksCache);
            CloseableUtils.closeQuietly(runsCache);
        }
    }
}
