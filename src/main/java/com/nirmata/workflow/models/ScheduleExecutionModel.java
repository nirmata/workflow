package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.util.Date;

public class ScheduleExecutionModel
{
    private final ScheduleId scheduleId;
    private final Date lastExecutionStartUtc;
    private final Date lastExecutionEndUtc;
    private final int executionQty;

    public ScheduleExecutionModel(ScheduleId scheduleId, Date lastExecutionStartUtc, Date lastExecutionEndUtc, int executionQty)
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

    public Date getLastExecutionStartUtc()
    {
        return lastExecutionStartUtc;
    }

    public Date getLastExecutionEndUtc()
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
