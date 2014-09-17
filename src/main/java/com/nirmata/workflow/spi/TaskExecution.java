package com.nirmata.workflow.spi;

import com.nirmata.workflow.models.TaskExecutionResultModel;

/**
 * Represents an execution task
 */
public interface TaskExecution
{
    /**
     * Execute the task and return the result when complete
     *
     * @return result
     */
    public TaskExecutionResultModel execute();
}
