package com.nirmata.workflow.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class TaskExecutionResult
{
    private final TaskExecutionStatus status;
    private final Map<String, String> resultData;

    public TaskExecutionResult(TaskExecutionStatus status, Map<String, String> resultData)
    {
        this.status = Preconditions.checkNotNull(status, "status cannot be null");
        resultData = Preconditions.checkNotNull(resultData, "resultData cannot be null");

        this.resultData = ImmutableMap.copyOf(resultData);
    }

    public TaskExecutionStatus getStatus()
    {
        return status;
    }

    public Map<String, String> getResultData()
    {
        return resultData;
    }
}
