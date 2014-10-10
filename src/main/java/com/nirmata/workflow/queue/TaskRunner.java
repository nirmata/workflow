package com.nirmata.workflow.queue;

import com.nirmata.workflow.models.ExecutableTask;

@FunctionalInterface
public interface TaskRunner
{
    public void executeTask(ExecutableTask executableTask);
}
