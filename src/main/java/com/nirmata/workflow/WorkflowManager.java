package com.nirmata.workflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.nirmata.workflow.details.ExecutableTaskRunner;
import com.nirmata.workflow.details.InternalJsonSerializer;
import com.nirmata.workflow.details.Scheduler;
import com.nirmata.workflow.details.StateCache;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.ScheduleModel;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowModel;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.zookeeper.ZooKeeperQueueFactory;
import com.nirmata.workflow.spi.JsonSerializer;
import com.nirmata.workflow.spi.StorageBridge;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.spi.TaskExecutor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The main container/manager for the Workflow
 */
public class WorkflowManager implements Closeable
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
    private final Queue idempotentTaskQueue;
    private final Queue nonIdempotentTaskQueue;
    private final List<QueueConsumer> taskConsumers;
    private final ExecutableTaskRunner executableTaskRunner;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * Create the workflow
     *
     * @param curator Curator instance namespaced as desired
     * @param configuration configuration
     * @param taskExecutor the task executor
     * @param storageBridge the storage bridge
     */
    public WorkflowManager(CuratorFramework curator, WorkflowManagerConfiguration configuration, TaskExecutor taskExecutor, StorageBridge storageBridge)
    {
        this(curator, configuration, taskExecutor, storageBridge, new ZooKeeperQueueFactory());
    }

    /**
     * Create the workflow
     *
     * @param curator Curator instance namespaced as desired
     * @param configuration configuration
     * @param taskExecutor the task executor
     * @param storageBridge the storage bridge
     * @param queueFactory the queue factory
     */
    public WorkflowManager(CuratorFramework curator, WorkflowManagerConfiguration configuration, TaskExecutor taskExecutor, StorageBridge storageBridge, QueueFactory queueFactory)
    {
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.configuration = Preconditions.checkNotNull(configuration, "configuration cannot be null");
        this.storageBridge = Preconditions.checkNotNull(storageBridge, "storageBridge cannot be null");
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");

        scheduler = new Scheduler(this);

        idempotentTaskQueue = queueFactory.createIdempotentQueue(this);
        nonIdempotentTaskQueue = queueFactory.createNonIdempotentQueue(this);
        taskConsumers = makeTaskConsumers(queueFactory, configuration);
        executableTaskRunner = new ExecutableTaskRunner(taskExecutor, curator);
    }

    /**
     * The workflow must be started
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        try
        {
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

    /**
     * Close the workflow when it's no longer needed. All running tasks, etc. will be closed.
     */
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
            CloseableUtils.closeQuietly(scheduler);
            scheduledExecutorService.shutdownNow();
        }
    }

    /**
     * Execute a task
     *
     * @param executableTask task to execute
     */
    public void executeTask(ExecutableTaskModel executableTask)
    {
        executableTaskRunner.executeTask(executableTask);
    }

    /**
     * Return the stored task result data or null
     *
     * @param scheduleId the schedule that the task was run
     * @param taskId the task
     * @return result data or null
     */
    public TaskExecutionResult getTaskExecutionResult(ScheduleId scheduleId, TaskId taskId)
    {
        String path = ZooKeeperConstants.getCompletedTaskPath(scheduleId, taskId);
        try
        {
            byte[] data = curator.getData().forPath(path);
            return InternalJsonSerializer.getTaskExecutionResult(JsonSerializer.fromBytes(data));
        }
        catch ( Exception e )
        {
            log.error(String.format("getTaskExecutionResult(%s, %s)", scheduleId, taskId), e);
        }
        return null;
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

    public StorageBridge getStorageBridge()
    {
        return storageBridge;
    }

    public Queue getIdempotentTaskQueue()
    {
        return idempotentTaskQueue;
    }

    public Queue getNonIdempotentTaskQueue()
    {
        return nonIdempotentTaskQueue;
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
