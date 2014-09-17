package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.nirmata.workflow.details.ExecutableTaskRunner;
import com.nirmata.workflow.details.Scheduler;
import com.nirmata.workflow.details.StateCache;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.zookeeper.ZooKeeperQueueFactory;
import com.nirmata.workflow.spi.StorageBridge;
import com.nirmata.workflow.spi.TaskExecutor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class WorkflowManager implements AutoCloseable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TaskExecutor taskExecutor;
    private final StorageBridge storageBridge;
    private final WorkflowManagerConfiguration configuration;
    private final CuratorFramework curator;
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("WorkflowManager");
    private final AtomicReference<StateCache> stateCache = new AtomicReference<StateCache>(new StateCache());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final Scheduler scheduler;
    private final PathChildrenCache completedTasksCache;
    private final Queue idempotentTaskQueue;
    private final Queue nonIdempotentTaskQueue;
    private final List<QueueConsumer> taskConsumers;
    private final ExecutableTaskRunner executableTaskRunner = new ExecutableTaskRunner(this);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManager(CuratorFramework curator, WorkflowManagerConfiguration configuration, TaskExecutor taskExecutor, StorageBridge storageBridge)
    {
        this(curator, configuration, taskExecutor, storageBridge, new ZooKeeperQueueFactory());
    }

    public WorkflowManager(CuratorFramework curator, WorkflowManagerConfiguration configuration, TaskExecutor taskExecutor, StorageBridge storageBridge, QueueFactory queueFactory)
    {
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.configuration = Preconditions.checkNotNull(configuration, "configuration cannot be null");
        this.storageBridge = Preconditions.checkNotNull(storageBridge, "storageBridge cannot be null");
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");

        scheduler = new Scheduler(this);

        completedTasksCache = new PathChildrenCache(curator, ZooKeeperConstants.COMPLETED_TASKS_PATH, true);

        idempotentTaskQueue = queueFactory.createIdempotentQueue(this);
        nonIdempotentTaskQueue = queueFactory.createNonIdempotentQueue(this);
        taskConsumers = makeTaskConsumers(queueFactory, configuration);
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        try
        {
            completedTasksCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            idempotentTaskQueue.start();
            nonIdempotentTaskQueue.start();
            for ( QueueConsumer queueConsumer : taskConsumers )
            {
                queueConsumer.start();
            }
        }
        catch ( Exception e )
        {
            log.error("Starting manager", e);
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

        scheduler.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(idempotentTaskQueue);
            CloseableUtils.closeQuietly(nonIdempotentTaskQueue);
            for ( QueueConsumer queueConsumer : taskConsumers )
            {
                CloseableUtils.closeQuietly(queueConsumer);
            }
            CloseableUtils.closeQuietly(completedTasksCache);
            CloseableUtils.closeQuietly(scheduler);
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

    public PathChildrenCache getCompletedTasksCache()
    {
        return completedTasksCache;
    }

    public StorageBridge getStorageBridge()
    {
        return storageBridge;
    }

    public void executeTask(ExecutableTaskModel executableTask)
    {
        executableTaskRunner.executeTask(executableTask);
    }

    private void updateState()
    {
        List<ScheduleModel> scheduleModels = storageBridge.getScheduleModels();
        List<TaskModel> taskModels = storageBridge.getTaskModels();
        List<WorkflowModel> workflowModels = storageBridge.getWorkflowModels();
        List<ScheduleExecutionModel> scheduleExecutions = storageBridge.getScheduleExecutions();
        stateCache.set(new StateCache(scheduleModels, scheduleExecutions, taskModels, workflowModels));
    }

    private List<QueueConsumer> makeTaskConsumers(QueueFactory queueFactory, WorkflowManagerConfiguration configuration)
    {
        ImmutableList.Builder<QueueConsumer> builder = ImmutableList.builder();
        for ( int i = 0; i < configuration.getIdempotentTaskQty(); ++i )
        {
            builder.add(queueFactory.createIdempotentQueueConsumer(this));
        }
        for ( int i = 0; i < configuration.getNonIdempotentTaskQty(); ++i )
        {
            builder.add(queueFactory.createNonIdempotentQueueConsumer(this));
        }
        return builder.build();
    }
}
