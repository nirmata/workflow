package com.nirmata.workflow.queue;

import com.nirmata.workflow.details.WorkflowManagerImpl;

public interface QueueFactory
{
    public Queue createQueue(WorkflowManagerImpl workflowManager, boolean idempotent);
    public QueueConsumer createQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, boolean idempotent);
}
