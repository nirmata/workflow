package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.time.LocalDateTime;

/**
 * Models an executing schedule
 */
public class ScheduleExecutionModel
{
    private final ScheduleId scheduleId;
    private final LocalDateTime lastExecutionStartUtc;
    private final LocalDateTime lastExecutionEndUtc;
    private final int executionQty;

    /**
     * @param scheduleId the schedule ID
     * @param lastExecutionStartUtc the start time UTC of last execution
     * @param lastExecutionEndUtc the end time UTC of last execution
     * @param executionQty the number of times this schedule has executed or 0
     */
    public ScheduleExecutionModel(ScheduleId scheduleId, LocalDateTime lastExecutionStartUtc, LocalDateTime lastExecutionEndUtc, int executionQty)
    {
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.lastExecutionStartUtc = Preconditions.checkNotNull(lastExecutionStartUtc, "lastExecution cannot be null");
        this.lastExecutionEndUtc = Preconditions.checkNotNull(lastExecutionEndUtc, "lastExecutionEnd cannot be null");
        this.executionQty = executionQty;
    }

    public ScheduleId getScheduleId()
    {
        return scheduleId;
    }

    public LocalDateTime getLastExecutionStartUtc()
    {
        return lastExecutionStartUtc;
    }

    public LocalDateTime getLastExecutionEndUtc()
    {
        return lastExecutionEndUtc;
    }

    public int getExecutionQty()
    {
        return executionQty;
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

        ScheduleExecutionModel that = (ScheduleExecutionModel)o;

        if ( executionQty != that.executionQty )
        {
            return false;
        }
        if ( !lastExecutionEndUtc.equals(that.lastExecutionEndUtc) )
        {
            return false;
        }
        if ( !lastExecutionStartUtc.equals(that.lastExecutionStartUtc) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !scheduleId.equals(that.scheduleId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = scheduleId.hashCode();
        result = 31 * result + lastExecutionStartUtc.hashCode();
        result = 31 * result + lastExecutionEndUtc.hashCode();
        result = 31 * result + executionQty;
        return result;
    }

    @Override
    public String toString()
    {
        return "ScheduleExecutionModel{" +
            "scheduleId=" + scheduleId +
            ", lastExecutionStart=" + lastExecutionStartUtc +
            ", lastExecutionEnd=" + lastExecutionEndUtc +
            ", executionQty=" + executionQty +
            '}';
    }
}
