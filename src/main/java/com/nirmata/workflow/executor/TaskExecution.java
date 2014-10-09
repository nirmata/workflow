package com.nirmata.workflow.executor;

/**
 * Represents an execution task
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
