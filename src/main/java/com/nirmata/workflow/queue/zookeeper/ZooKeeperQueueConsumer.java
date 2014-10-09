package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.ExecutableTaskModel;
import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.queue.QueueConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperQueueConsumer implements QueueConsumer, org.apache.curator.framework.recipes.queue.QueueConsumer<ExecutableTaskModel>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DistributedQueue<ExecutableTaskModel> queue;
    private final WorkflowManagerImpl workflowManager;

    public ZooKeeperQueueConsumer(WorkflowManagerImpl workflowManager, boolean idempotent)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        String path = idempotent ? ZooKeeperConstants.getIdempotentTasksQueuePath(): ZooKeeperConstants.getNonIdempotentTasksQueuePath();
        QueueBuilder<ExecutableTaskModel> builder = QueueBuilder.builder(workflowManager.getCurator(), this, new TaskQueueSerializer(), path);
        if ( idempotent )
        {
            builder = builder.lockPath(ZooKeeperConstants.getIdempotentTasksQueueLockPath());
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
    public void consumeMessage(ExecutableTaskModel executableTask)
    {
        workflowManager.executeTask(executableTask);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        // other parts of the library will handle
    }
}
