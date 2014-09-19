package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.ExecutableTaskModel;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    /**
     * Create a new task execution for the given task
     *
     * @param task task
     * @return execution
     */
    public TaskExecution newTaskExecution(ExecutableTaskModel task);
}
