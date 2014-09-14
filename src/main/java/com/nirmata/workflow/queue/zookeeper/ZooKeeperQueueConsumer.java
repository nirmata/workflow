package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.queue.QueueConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;

public class ZooKeeperQueueConsumer implements QueueConsumer, org.apache.curator.framework.recipes.queue.QueueConsumer<ExecutableTaskModel>
{
    private final DistributedQueue<ExecutableTaskModel> queue;
    private final WorkflowManager workflowManager;

    public ZooKeeperQueueConsumer(WorkflowManager workflowManager, boolean idempotent)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
        String path = idempotent ? ZooKeeperConstants.IDEMPOTENT_TASKS_QUEUE_PATH : ZooKeeperConstants.NON_IDEMPOTENT_TASKS_QUEUE_PATH;
        QueueBuilder<ExecutableTaskModel> builder = QueueBuilder.builder(workflowManager.getCurator(), this, new TaskQueueSerializer(), path);
        if ( idempotent )
        {
            builder = builder.lockPath(ZooKeeperConstants.IDEMPOTENT_TASKS_QUEUE_LOCK_PATH);
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
            // TODO log
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
