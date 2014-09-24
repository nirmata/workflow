package com.nirmata.workflow.details;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.spi.TaskExecutionStatus;

class RunIdWithStatus
{
    private final RunId runId;
    private final TaskExecutionStatus status;

    RunIdWithStatus(RunId runId, TaskExecutionStatus status)
    {
        this.runId = runId;
        this.status = status;
    }

    RunId getRunId()
    {
        return runId;
    }

    TaskExecutionStatus getStatus()
    {
        return status;
    }
}
