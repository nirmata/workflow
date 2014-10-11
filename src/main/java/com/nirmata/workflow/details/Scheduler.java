package com.nirmata.workflow.details;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
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
        BlockingQueue<RunId> updatedRunIds = Queues.newLinkedBlockingQueue();
        completedTasksCache.getListenable().addListener((client, event) -> {
            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
            {
                RunId runId = new RunId(ZooKeeperConstants.getRunIdFromCompletedTasksPath(event.getData().getPath()));
                updatedRunIds.add(runId);
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
                RunId runId = updatedRunIds.take();
                updateTasks(runId);
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

    private boolean hasCanceledTasks(RunId runId, RunnableTask runnableTask)
    {
        return runnableTask.getTasks().keySet().stream().anyMatch(taskId -> {
            String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
            ChildData currentData = completedTasksCache.getCurrentData(completedTaskPath);
            if ( currentData != null )
            {
                TaskExecutionResult taskExecutionResult = JsonSerializer.getTaskExecutionResult(JsonSerializer.fromBytes(currentData.getData()));
                return taskExecutionResult.getStatus().isCancelingStatus();
            }
            return false;
        });
    }

    static void completeTask(Logger log, WorkflowManagerImpl workflowManager, RunId runId, RunnableTask runnableTask, int version)
    {
        RunnableTask completedRunnableTask = new RunnableTask(runnableTask.getTasks(), runnableTask.getTaskDags(), runnableTask.getStartTime(), LocalDateTime.now(Clock.systemUTC()));
        String runPath = ZooKeeperConstants.getRunPath(runId);
        byte[] json = JsonSerializer.toBytes(JsonSerializer.newRunnableTask(completedRunnableTask));
        try
        {
            workflowManager.getCurator().setData().withVersion(version).forPath(runPath, json);
        }
        catch ( Exception e )
        {
            String message = "Could not write completed task data for run: " + runId;
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    private void updateTasks(RunId runId)
    {
        ChildData currentData = runsCache.getCurrentData(ZooKeeperConstants.getRunPath(runId));
        if ( currentData == null )
        {
            String message = "Could not find run for RunId: " + runId;
            log.error(message);
            throw new RuntimeException(message);
        }
        RunnableTask runnableTask = JsonSerializer.getRunnableTask(JsonSerializer.fromBytes(currentData.getData()));
        if ( runnableTask.getCompletionTime().isPresent() )
        {
            return;
        }

        if ( hasCanceledTasks(runId, runnableTask) )
        {
            completeTask(log, workflowManager, runId, runnableTask, -1);
            return; // one or more tasks has canceled the entire run
        }

        Set<TaskId> completedTasks = Sets.newHashSet();
        runnableTask.getTaskDags().forEach(entry -> {
            TaskId taskId = entry.getTaskId();
            ExecutableTask task = runnableTask.getTasks().get(taskId);
            if ( task == null )
            {
                log.error(String.format("Could not find task: %s for run: %s", taskId, runId));
                return;
            }
            
            boolean taskIsComplete = taskIsComplete(completedTasksCache, runId, task);
            if ( taskIsComplete )
            {
                completedTasks.add(taskId);
            }
            else if ( !taskIsStarted(startedTasksCache, runId, taskId) )
            {
                boolean allDependenciesAreComplete = entry
                    .getDependencies()
                    .stream()
                    .allMatch(id -> taskIsComplete(completedTasksCache, runId, runnableTask.getTasks().get(id)));
                if ( allDependenciesAreComplete )
                {
                    queueTask(runId, task);
                }
            }
        });

        if ( completedTasks.equals(runnableTask.getTasks().keySet()))
        {
            completeTask(log, workflowManager, runId, runnableTask, -1);
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

    private boolean taskIsComplete(PathChildrenCache completedTasksCache, RunId runId, ExecutableTask task)
    {
        if ( (task == null) || !task.isExecutable() )
        {
            return true;
        }
        String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, task.getTaskId());
        return (completedTasksCache.getCurrentData(completedTaskPath) != null);
    }

    private Map<TaskType, Queue> makeTaskQueues(List<TaskExecutorSpec> specs)
    {
        ImmutableMap.Builder<TaskType, Queue> builder = ImmutableMap.builder();
        specs.forEach(spec -> {
            Queue queue = queueFactory.createQueue(workflowManager, spec.getTaskType());
            builder.put(spec.getTaskType(), queue);
        });
        return builder.build();
    }
}
