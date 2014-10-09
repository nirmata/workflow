package com.nirmata.workflow;

import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueFactory;
import org.apache.curator.framework.CuratorFramework;
import java.util.Collection;

public class WorkflowManagerBuilder
{
    public static WorkflowManagerBuilder build()
    {
        return null;
    }

    public WorkflowManagerBuilder withCurator(CuratorFramework curator)
    {
        return this;
    }

    public WorkflowManagerBuilder withQueueFactory(QueueFactory queueFactory)
    {
        return this;
    }

    public WorkflowManagerBuilder withInstanceName(String instanceName)
    {
        return this;
    }

    public WorkflowManagerBuilder withIdempotentTaskExecutors(TaskExecutor taskExecutor, int qty, Collection<TaskType> limitTaskTypes)
    {
        return null;
    }

    public WorkflowManagerBuilder withNonIdempotentTaskExecutors(TaskExecutor taskExecutor, int qty, Collection<TaskType> limitTaskTypes)
    {
        return null;
    }

    public WorkflowManager andCreate()
    {
        return null;
    }

    private WorkflowManagerBuilder()
    {
    }
}
