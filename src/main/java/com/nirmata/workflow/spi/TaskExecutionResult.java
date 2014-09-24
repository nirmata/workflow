package com.nirmata.workflow.spi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * A task result
 */
public class TaskExecutionResult
{
    private final String details;
    private final Map<String, String> resultData;
    private final LocalDateTime completionDateUtc;
    private final TaskExecutionStatus status;

    /**
     * @param status result status
     * @param details task-specific details
     * @param resultData task-specific fields/values
     */
    public TaskExecutionResult(TaskExecutionStatus status, String details, Map<String, String> resultData)
    {
        this(status, details, resultData, LocalDateTime.now(Clock.systemUTC()));
    }

    /**
     * @param status result status
     * @param details task-specific details
     * @param resultData task-specific fields/values
     * @param completionDateUtc date of completion
     */
    public TaskExecutionResult(TaskExecutionStatus status, String details, Map<String, String> resultData, LocalDateTime completionDateUtc)
    {
        resultData = Preconditions.checkNotNull(resultData, "resultData cannot be null");
        this.status = Preconditions.checkNotNull(status, "status cannot be null");
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

    public LocalDateTime getCompletionDateUtc()
    {
        return completionDateUtc;
    }

    public TaskExecutionStatus getStatus()
    {
        return status;
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
        int result = details.hashCode();
        result = 31 * result + resultData.hashCode();
        result = 31 * result + completionDateUtc.hashCode();
        result = 31 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskExecutionResult{" +
            "details='" + details + '\'' +
            ", resultData=" + resultData +
            ", completionDateUtc=" + completionDateUtc +
            ", status=" + status +
            '}';
    }
}
