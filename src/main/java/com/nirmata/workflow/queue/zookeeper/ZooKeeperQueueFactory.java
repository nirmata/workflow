package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.queue.TaskRunner;

public class ZooKeeperQueueFactory implements QueueFactory
{
    @Override
    public Queue createQueue(WorkflowManagerImpl workflowManager, TaskType taskType)
    {
        return new ZooKeeperQueue(workflowManager.getCurator(), taskType);
    }

    @Override
    public QueueConsumer createQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, TaskType taskType)
    {
        return new ZooKeeperQueueConsumer(workflowManager, taskRunner, taskType);
    }
}
