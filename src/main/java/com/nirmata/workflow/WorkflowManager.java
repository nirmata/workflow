package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.Scheduler;
import com.nirmata.workflow.details.StateCache;
import com.nirmata.workflow.details.TaskRunner;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.StorageBridge;
import com.nirmata.workflow.spi.TaskExecutor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkflowManager implements AutoCloseable
{
    private final TaskExecutor taskExecutor;
    private final StorageBridge storageBridge;
    private final WorkflowManagerConfiguration configuration;
    private final CuratorFramework curator;
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("WorkflowManager");
    private final AtomicReference<StateCache> stateCache = new AtomicReference<StateCache>(new StateCache());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final Scheduler scheduler;
    private final TaskRunner taskRunner;
    private final PathChildrenCache schedulesCache;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManager(CuratorFramework curator, WorkflowManagerConfiguration configuration, TaskExecutor taskExecutor, StorageBridge storageBridge)
    {
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.configuration = Preconditions.checkNotNull(configuration, "configuration cannot be null");
        this.storageBridge = Preconditions.checkNotNull(storageBridge, "storageBridge cannot be null");
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");

        taskRunner = (configuration.getMaxTaskRunners() > 0) ? new TaskRunner(this) : null;
        scheduler = configuration.canBeScheduler() ? new Scheduler(this) : null;

        schedulesCache = new PathChildrenCache(curator, ZooKeeperConstants.SCHEDULES_PATH, true);
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        try
        {
            schedulesCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        }
        catch ( Exception e )
        {
            // TODO
            throw new RuntimeException(e);
        }

        Runnable stateUpdater = new Runnable()
        {
            @Override
            public void run()
            {
                updateState();
            }
        };
        scheduledExecutorService.scheduleWithFixedDelay(stateUpdater, configuration.getStorageRefreshMs(), configuration.getStorageRefreshMs(), TimeUnit.MILLISECONDS);

        if ( scheduler != null )
        {
            scheduler.start();
        }

        if ( taskRunner != null )
        {
            taskRunner.start();
        }
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(schedulesCache);
            CloseableUtils.closeQuietly(scheduler);
            CloseableUtils.closeQuietly(taskRunner);
            scheduledExecutorService.shutdownNow();
        }
    }

    public CuratorFramework getCurator()
    {
        return curator;
    }

    public WorkflowManagerConfiguration getConfiguration()
    {
        return configuration;
    }

    public StateCache getStateCache()
    {
        return stateCache.get();
    }

    public TaskExecutor getTaskExecutor()
    {
        return taskExecutor;
    }

    public PathChildrenCache getSchedulesCache()
    {
        return schedulesCache;
    }

    private void updateState()
    {
        List<ScheduleModel> scheduleModels = storageBridge.getScheduleModels();
        List<TaskModel> taskModels = storageBridge.getTaskModels();
        List<WorkflowModel> workflowModels = storageBridge.getWorkflowModels();
        List<ScheduleExecutionModel> scheduleExecutions = storageBridge.getScheduleExecutions();
        stateCache.set(new StateCache(scheduleModels, scheduleExecutions, taskModels, workflowModels));
    }
}
