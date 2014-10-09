package com.nirmata.workflow.queue;

import com.nirmata.workflow.details.WorkflowManagerImpl;

public interface QueueFactory
{
    public Queue createIdempotentQueue(WorkflowManagerImpl workflowManager);
    public Queue createNonIdempotentQueue(WorkflowManagerImpl workflowManager);
    public QueueConsumer createIdempotentQueueConsumer(WorkflowManagerImpl workflowManager);
    public QueueConsumer createNonIdempotentQueueConsumer(WorkflowManagerImpl workflowManager);
}
