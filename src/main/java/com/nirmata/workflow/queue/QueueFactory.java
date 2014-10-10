package com.nirmata.workflow.queue;

import com.nirmata.workflow.details.WorkflowManagerImpl;
import com.nirmata.workflow.models.TaskType;

public interface QueueFactory
{
    public Queue createQueue(WorkflowManagerImpl workflowManager, TaskType taskType);
    public QueueConsumer createQueueConsumer(WorkflowManagerImpl workflowManager, TaskRunner taskRunner, TaskType taskType);
}
