package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

class Scheduler
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerImpl workflowManager;
    private final QueueFactory queueFactory;
    private final Map<TaskType, Queue> queues;
    private final PathChildrenCache completedTasksCache;
    private final PathChildrenCache startedTasksCache;
    private final PathChildrenCache runsCache;

    Scheduler(WorkflowManagerImpl workflowManager, QueueFactory queueFactory, List<TaskExecutorSpec> specs)
    {
        this.workflowManager = workflowManager;
        this.queueFactory = queueFactory;
        queues = makeTaskQueues(specs);

        completedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getCompletedTaskParentPath(), true);
        startedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getStartedTasksParentPath(), false);
        runsCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.getRunParentPath(), true);
    }

    void run()
    {
        BlockingQueue<RunIdWithStatus> updatedRunIds = Queues.newLinkedBlockingQueue();
        completedTasksCache.getListenable().addListener((client, event) -> {
            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
            {
                TaskExecutionResult taskExecutionResult = JsonSerializer.getTaskExecutionResult(JsonSerializer.fromBytes(event.getData().getData()));
                RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                completedTasksCache.clearDataBytes(event.getData().getPath());
                updatedRunIds.add(new RunIdWithStatus(runId, taskExecutionResult.getStatus()));
            }
        });

        try
        {
            queues.values().forEach(Queue::start);
            completedTasksCache.start(PathChildrenCache.StartMode.NORMAL);
            startedTasksCache.start(PathChildrenCache.StartMode.NORMAL);
            runsCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            while ( !Thread.currentThread().isInterrupted() )
            {
                RunIdWithStatus idWithStatus = updatedRunIds.take();
                updateTasks(idWithStatus.getRunId(), idWithStatus.getStatus());
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Throwable e )
        {
            log.error("Error while running scheduler", e);
        }
        finally
        {
            queues.values().forEach(CloseableUtils::closeQuietly);
            CloseableUtils.closeQuietly(completedTasksCache);
            CloseableUtils.closeQuietly(startedTasksCache);
            CloseableUtils.closeQuietly(runsCache);
        }
    }

    private void updateTasks(RunId runId, TaskExecutionStatus status)
    {
        if ( status.isCancelingStatus() )
        {
            // TODO complete task
        }

        ChildData currentData = runsCache.getCurrentData(ZooKeeperConstants.getRunPath(runId));
        if ( currentData == null )
        {
            log.warn("Could not find run for RunId: " + runId);
            return; // it must have been canceled
        }
        RunnableTask runnableTask = JsonSerializer.getRunnableTask(JsonSerializer.fromBytes(currentData.getData()));

        Set<TaskId> completedTasks = Sets.newHashSet();
        runnableTask.getTaskDags().forEach(entry -> {
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
                    queueTask(runId, runnableTask.getTasks().get(taskId));
                }
            }
        });

        Set<TaskId> entries = null; // TODO workflow.getRunnableTaskDag().getEntries().stream().map(RunnableTaskDagEntryModel::getTaskId).collect(Collectors.toSet());
        if ( completedTasks.equals(entries))
        {
            // TODO complete task
        }
    }

    private void queueTask(RunId runId, ExecutableTask task)
    {
        String path = ZooKeeperConstants.getStartedTaskPath(runId, task.getTaskId());
        try
        {
            StartedTask startedTask = new StartedTask(workflowManager.getInstanceName(), LocalDateTime.now(Clock.systemUTC()));
            byte[] data = JsonSerializer.toBytes(JsonSerializer.newStartedTask(startedTask));
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path, data);
            Queue queue = queues.get(task.getTaskType());
            if ( queue == null )
            {
                throw new Exception("Could not find a queue for the type: " + task.getTaskType());
            }
            queue.put(task);
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

    private Map<TaskType, Queue> makeTaskQueues(List<TaskExecutorSpec> specs)
    {
        ImmutableMap.Builder<TaskType, Queue> builder = ImmutableMap.builder();
        specs.forEach(spec -> {
            Queue queue = queueFactory.createQueue(workflowManager, spec.getTaskType().isIdempotent());
            builder.put(spec.getTaskType(), queue);
        });
        return builder.build();
    }
}
