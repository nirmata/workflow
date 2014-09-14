package com.nirmata.workflow.spi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class TaskExecutionResult
{
    private final String details;
    private final Map<String, String> resultData;

    public TaskExecutionResult(String details, Map<String, String> resultData)
    {
        resultData = Preconditions.checkNotNull(resultData, "resultData cannot be null");
        this.details = Preconditions.checkNotNull(details, "details cannot be null");
        this.resultData = ImmutableMap.copyOf(resultData);
    }

    public String getDetails()
    {
        return details;
    }

    public Map<String, String> getResultData()
    {
        return resultData;
    }
}
