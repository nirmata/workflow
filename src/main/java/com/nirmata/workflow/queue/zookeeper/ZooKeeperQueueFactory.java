package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;

public class ZooKeeperQueueFactory implements QueueFactory
{
    @Override
    public Queue createIdempotentQueue(WorkflowManager workflowManager)
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), true);
    }

    @Override
    public Queue createNonIdempotentQueue(WorkflowManager workflowManager)
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), false);
    }

    @Override
    public QueueConsumer createIdempotentQueueConsumer(WorkflowManager workflowManager)
    {
        return new ZooKeeperQueueConsumer(workflowManager, true);
    }

    @Override
    public QueueConsumer createNonIdempotentQueueConsumer(WorkflowManager workflowManager)
    {
        return new ZooKeeperQueueConsumer(workflowManager, false);
    }
}
