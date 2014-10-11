package com.nirmata.workflow.admin;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.RunId;
import java.time.LocalDateTime;
import java.util.Optional;

public class RunInfo
{
    private final RunId runId;
    private final LocalDateTime startTimeUtc;
    private final Optional<LocalDateTime> completionTimeUtc;

    public RunInfo(RunId runId, LocalDateTime startTimeUtc)
    {
        this(runId, startTimeUtc, null);
    }

    public RunInfo(RunId runId, LocalDateTime startTimeUtc, LocalDateTime completionTimeUtc)
    {
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        this.startTimeUtc = Preconditions.checkNotNull(startTimeUtc, "startTimeUtc cannot be null");
        this.completionTimeUtc = Optional.ofNullable(completionTimeUtc);
    }

    public RunId getRunId()
    {
        return runId;
    }

    public LocalDateTime getStartTimeUtc()
    {
        return startTimeUtc;
    }

    public LocalDateTime getCompletionTimeUtc()
    {
        return completionTimeUtc.get();
    }

    public boolean isComplete()
    {
        return completionTimeUtc.isPresent();
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

        if ( !completionTimeUtc.equals(runInfo.completionTimeUtc) )
        {
            return false;
        }
        if ( !runId.equals(runInfo.runId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !startTimeUtc.equals(runInfo.startTimeUtc) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = runId.hashCode();
        result = 31 * result + startTimeUtc.hashCode();
        result = 31 * result + completionTimeUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunInfo{" +
            "runId=" + runId +
            ", startTime=" + startTimeUtc +
            ", completionTime=" + completionTimeUtc +
            '}';
    }
}
