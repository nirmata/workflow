package com.nirmata.workflow.executor;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.ExecutableTask;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask executableTask);
}
