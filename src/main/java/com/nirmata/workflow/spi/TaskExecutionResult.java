package com.nirmata.workflow.spi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Date;
import java.util.Map;

/**
 * A task result
 */
public class TaskExecutionResult
{
    private final String details;
    private final Map<String, String> resultData;
    private final Date completionDateUtc;

    /**
     * @param details task-specific details
     * @param resultData task-specific fields/values
     */
    public TaskExecutionResult(String details, Map<String, String> resultData)
    {
        this(details, resultData, Clock.nowUtc());
    }

    /**
     * @param details task-specific details
     * @param resultData task-specific fields/values
     * @param completionDateUtc date of completion
     */
    public TaskExecutionResult(String details, Map<String, String> resultData, Date completionDateUtc)
    {
        resultData = Preconditions.checkNotNull(resultData, "resultData cannot be null");
        this.completionDateUtc = Preconditions.checkNotNull(completionDateUtc, "completionDateUtc cannot be null");
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

    public Date getCompletionDateUtc()
    {
        return completionDateUtc;
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

        if ( !completionDateUtc.equals(that.completionDateUtc) )
        {
            return false;
        }
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
        result = 31 * result + completionDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskExecutionResult{" +
            "details='" + details + '\'' +
            ", resultData=" + resultData +
            ", completionDateUtc=" + completionDateUtc +
            '}';
    }
}
