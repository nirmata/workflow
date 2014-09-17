package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.TaskModel;

/**
 * Factory for creating task executions
 */
public interface TaskExecutor
{
    /**
     * Create a new task execution for the given task
     *
     * @param task task
     * @return execution
     */
    public TaskExecution newTaskExecution(TaskModel task);
}
