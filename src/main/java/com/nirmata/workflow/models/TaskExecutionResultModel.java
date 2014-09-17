package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * A task result
 */
public class TaskExecutionResultModel
{
    private final String details;
    private final Map<String, String> resultData;

    /**
     * @param details task-specific details
     * @param resultData task-specific fields/values
     */
    public TaskExecutionResultModel(String details, Map<String, String> resultData)
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

        TaskExecutionResultModel that = (TaskExecutionResultModel)o;

        if ( !details.equals(that.details) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !resultData.equals(that.resultData) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = details.hashCode();
        result = 31 * result + resultData.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskExecutionResultModel{" +
            "details='" + details + '\'' +
            ", resultData=" + resultData +
            '}';
    }
}
