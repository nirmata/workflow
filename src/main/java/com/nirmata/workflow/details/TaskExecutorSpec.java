package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;
import java.util.Collection;

public class TaskExecutorSpec
{
    private final TaskExecutor taskExecutor;
    private final int qty;
    private final Collection<TaskType> taskTypes;

    public TaskExecutorSpec(TaskExecutor taskExecutor, int qty, Collection<TaskType> taskTypes)
    {
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");
        this.qty = qty;
        taskTypes = Preconditions.checkNotNull(taskTypes, "taskTypes cannot be null");
        this.taskTypes = ImmutableSet.copyOf(taskTypes);
    }

    public TaskExecutor getTaskExecutor()
    {
        return taskExecutor;
    }

    public int getQty()
    {
        return qty;
    }

    public Collection<TaskType> getTaskTypes()
    {
        return taskTypes;
    }
}
