package com.nirmata.workflow.executor;

import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    public TaskExecution newTaskExecution(RunId runId, ExecutableTask executableTask);
}
