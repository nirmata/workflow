package com.nirmata.workflow.executor;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.ExecutableTask;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    /**
     * Create a task execution for the given task
     *
     * @param workflowManager the manager
     * @param executableTask the task
     * @return the execution
     */
    public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask executableTask);
}
