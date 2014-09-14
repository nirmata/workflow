package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;

public class ZooKeeperQueueFactory implements QueueFactory
{
    private final WorkflowManager workflowManager;

    public ZooKeeperQueueFactory(WorkflowManager workflowManager)
    {
        this.workflowManager = Preconditions.checkNotNull(workflowManager, "workflowManager cannot be null");
    }

    @Override
    public Queue createIdempotentQueue()
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), true);
    }

    @Override
    public Queue createNonIdempotentQueue()
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), false);
    }

    @Override
    public QueueConsumer createIdempotentQueueConsumer()
    {
        return new ZooKeeperQueueConsumer(workflowManager, true);
    }

    @Override
    public QueueConsumer createNonIdempotentQueueConsumer()
    {
        return new ZooKeeperQueueConsumer(workflowManager, false);
    }
}
