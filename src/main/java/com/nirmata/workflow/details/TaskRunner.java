package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.spi.TaskExecution;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class TaskRunner implements Closeable
{
    private final WorkflowManager workflowManager;
    private final ExecutorService executorService;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final PathChildrenCache completedTasksCache;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public TaskRunner(WorkflowManager workflowManager)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        executorService = ThreadUtils.newFixedThreadPool(workflowManager.getConfiguration().getMaxTaskRunners() + 1, "TaskRunner");
        completedTasksCache = new PathChildrenCache(workflowManager.getCurator(), ZooKeeperConstants.COMPLETED_TASKS_PATH, false);
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        try
        {
            completedTasksCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        }
        catch ( Exception e )
        {
            // TODO
        }
        Runnable runner = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    while ( !Thread.currentThread().isInterrupted() )
                    {
                        Thread.sleep(workflowManager.getConfiguration().getTaskRunnerSleepMs());
                    }
                }
                catch ( InterruptedException dummy )
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        executorService.submit(runner);
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(completedTasksCache);
            executorService.shutdownNow();
        }
    }

    private void checkForTasks()
    {
        for ( ChildData data : workflowManager.getSchedulesCache().getCurrentData() )
        {
            ScheduleId scheduleId = new ScheduleId(ZKPaths.getNodeFromPath(data.getPath()));
        }
    }

    private void attemptToRunTask(ScheduleId scheduleId, TaskModel task)
    {
        String path = ZooKeeperConstants.getCompletedTaskKey(scheduleId, task.getTaskId());
        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path);

            TaskExecution taskExecution = workflowManager.getTaskExecutor().newTaskExecution(task);
            // TODO

            if ( !task.isIdempotent() )
            {
                workflowManager.getCurator().create().creatingParentsIfNeeded().forPath(path);
            }
        }
        catch ( KeeperException.NodeExistsException dummy )
        {
            // ignore - another process is executing the task
            // TODO log?
        }
        catch ( Exception e )
        {
            // TODO
        }
        finally
        {
            try
            {
                workflowManager.getCurator().delete().guaranteed().inBackground().forPath(path);
            }
            catch ( Exception e )
            {
                // TODO
            }
        }
    }

    private void attemptToTakeTask(ScheduleId scheduleId, TaskModel task)
    {
        String path = ZooKeeperConstants.getTaskLockKey(scheduleId, task.getTaskId());
        boolean hasLock = false;
        try
        {
            workflowManager.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
            hasLock = true;

            attemptToRunTask(scheduleId, task);
            // TODO
        }
        catch ( KeeperException.NodeExistsException dummy )
        {
            // ignore - another process has the task
        }
        catch ( Exception e )
        {
            // TODO
        }
        finally
        {
            if ( hasLock )
            {
                try
                {
                    workflowManager.getCurator().delete().guaranteed().inBackground().forPath(path);
                }
                catch ( Exception e )
                {
                    // TODO
                }
            }
        }
    }
}
