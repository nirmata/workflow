package com.nirmata.workflow.admin;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.RunId;
import java.time.LocalDateTime;
import java.util.Optional;

public class RunInfo
{
    private final RunId runId;
    private final LocalDateTime startTime;
    private final Optional<LocalDateTime> completionTime;

    public RunInfo(RunId runId, LocalDateTime startTime)
    {
        this(runId, startTime, null);
    }

    public RunInfo(RunId runId, LocalDateTime startTime, LocalDateTime completionTime)
    {
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        this.startTime = Preconditions.checkNotNull(startTime, "startTime cannot be null");
        this.completionTime = Optional.ofNullable(completionTime);
    }

    public RunId getRunId()
    {
        return runId;
    }

    public LocalDateTime getStartTime()
    {
        return startTime;
    }

    public LocalDateTime getCompletionTime()
    {
        return completionTime.get();
    }

    public boolean isComplete()
    {
        return completionTime.isPresent();
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

        RunInfo runInfo = (RunInfo)o;

        if ( !completionTime.equals(runInfo.completionTime) )
        {
            return false;
        }
        if ( !runId.equals(runInfo.runId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !startTime.equals(runInfo.startTime) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = runId.hashCode();
        result = 31 * result + startTime.hashCode();
        result = 31 * result + completionTime.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunInfo{" +
            "runId=" + runId +
            ", startTime=" + startTime +
            ", completionTime=" + completionTime +
            '}';
    }
}
