package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.Scheduler;
import com.nirmata.workflow.details.StateCache;
import com.nirmata.workflow.details.TaskRunner;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.spi.StorageBridge;
import com.nirmata.workflow.spi.TaskExecutor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import java.util.List;
import java.util.concurrent.ExecutorService;
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
    private final ExecutorService executorService;

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

        int threadQty = configuration.getMaxTaskRunners() + (configuration.canBeScheduler() ? 1 : 0) + ((configuration.getMaxTaskRunners() > 0) ? 1 : 0);
        executorService = (threadQty > 0) ? ThreadUtils.newFixedThreadPool(threadQty, "WorkflowManager") : null;
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        Runnable stateUpdater = new Runnable()
        {
            @Override
            public void run()
            {
                updateState();
            }
        };
        scheduledExecutorService.scheduleWithFixedDelay(stateUpdater, configuration.getStorageRefreshMs(), configuration.getStorageRefreshMs(), TimeUnit.MILLISECONDS);

        if ( configuration.canBeScheduler() )
        {
            executorService.submit(new Scheduler(this));
        }

        if ( configuration.getMaxTaskRunners() > 0 )
        {
            executorService.submit(new TaskRunner((this)));
        }
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            scheduledExecutorService.shutdownNow();
            if ( executorService != null )
            {
                executorService.shutdownNow();
            }
        }
    }

    private void updateState()
    {
        List<ScheduleModel> scheduleModels = storageBridge.getScheduleModels();
        List<TaskModel> taskModels = storageBridge.getTaskModels();
        List<WorkflowModel> workflowModels = storageBridge.getWorkflowModels();
        stateCache.set(new StateCache(scheduleModels, taskModels, workflowModels));
    }
}
