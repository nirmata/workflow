package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;

public class TaskExecutorSpec
{
    private final TaskExecutor taskExecutor;
    private final int qty;
    private final TaskType taskType;

    public TaskExecutorSpec(TaskExecutor taskExecutor, int qty, TaskType taskType)
    {
        this.taskType = Preconditions.checkNotNull(taskType, "taskType cannot be null");
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");
        this.qty = qty;
    }

    public TaskExecutor getTaskExecutor()
    {
        return taskExecutor;
    }

    public int getQty()
    {
        return qty;
    }

    public TaskType getTaskType()
    {
        return taskType;
    }
}
