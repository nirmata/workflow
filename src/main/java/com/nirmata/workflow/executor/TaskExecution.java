package com.nirmata.workflow.executor;

import com.nirmata.workflow.models.TaskExecutionResult;

/**
 * Represents a task execution. A new task execution is allocated for each run of a
 * task. The Workflow manager will call {@link #execute()} when the task should perform
 * its operation.
 */
@FunctionalInterface
public interface TaskExecution
{
    /**
     * Execute the task and return the result when complete
     *
     * @return result
     */
    public TaskExecutionResult execute();
}
