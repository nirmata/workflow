package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.TaskModel;

public interface TaskExecutor
{
    public TaskExecution newTaskExecution(TaskModel task);
}
