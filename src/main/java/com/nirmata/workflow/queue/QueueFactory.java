package com.nirmata.workflow.queue;

import com.nirmata.workflow.WorkflowManager;

public interface QueueFactory
{
    public Queue createIdempotentQueue(WorkflowManager workflowManager);
    public Queue createNonIdempotentQueue(WorkflowManager workflowManager);
    public QueueConsumer createIdempotentQueueConsumer(WorkflowManager workflowManager);
    public QueueConsumer createNonIdempotentQueueConsumer(WorkflowManager workflowManager);
}
