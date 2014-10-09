package com.nirmata.workflow.executor;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    public TaskExecution newTaskExecution(RunId runId, TaskId taskId, TaskType taskType);
}
