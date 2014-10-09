package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;

public class ZooKeeperQueueFactory implements QueueFactory
{
    @Override
    public Queue createIdempotentQueue(WorkflowManagerImpl workflowManager)
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), true);
    }

    @Override
    public Queue createNonIdempotentQueue(WorkflowManagerImpl workflowManager)
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), false);
    }

    @Override
    public QueueConsumer createIdempotentQueueConsumer(WorkflowManagerImpl workflowManager)
    {
        return new ZooKeeperQueueConsumer(workflowManager, true);
    }

    @Override
    public QueueConsumer createNonIdempotentQueueConsumer(WorkflowManagerImpl workflowManager)
    {
        return new ZooKeeperQueueConsumer(workflowManager, false);
    }
}
