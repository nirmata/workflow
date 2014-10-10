package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import java.util.Map;

public class TaskExecutionResult
{
    private final TaskExecutionStatus status;
    private final Map<String, String> resultData;

    public TaskExecutionResult(TaskExecutionStatus status)
    {
        this(status, Maps.newHashMap());
    }

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

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TaskExecutionResult that = (TaskExecutionResult)o;

        if ( !resultData.equals(that.resultData) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( status != that.status )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = status.hashCode();
        result = 31 * result + resultData.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskExecutionResult{" +
            "status=" + status +
            ", resultData=" + resultData +
            '}';
    }
}
