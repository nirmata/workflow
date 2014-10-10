package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperQueueConsumer implements QueueConsumer, org.apache.curator.framework.recipes.queue.QueueConsumer<ExecutableTask>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DistributedQueue<ExecutableTask> queue;
    private final TaskRunner taskRunner;

    public ZooKeeperQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, TaskType taskType)
    {
        this.taskRunner = Preconditions.checkNotNull(taskRunner, "taskRunner cannot be null");
        workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        String path = ZooKeeperConstants.getQueuePath(taskType);
        QueueBuilder<ExecutableTask> builder = QueueBuilder.builder(workflowManager.getCurator(), this, new TaskQueueSerializer(), path);
        if ( taskType.isIdempotent() )
        {
            builder = builder.lockPath(ZooKeeperConstants.getQueueLockPath(taskType));
        }
        queue = builder.buildQueue();
    }

    @Override
    public void start()
    {
        try
        {
            queue.start();
        }
        catch ( Exception e )
        {
            log.error("Could not start queue", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(queue);
    }

    @Override
    public void consumeMessage(ExecutableTask executableTask)
    {
        taskRunner.executeTask(executableTask);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        // other parts of the library will handle
    }
}
