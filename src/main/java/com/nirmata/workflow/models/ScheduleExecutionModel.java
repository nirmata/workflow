package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.util.Date;

public class ScheduleExecutionModel
{
    private final ScheduleId scheduleId;
    private final Date lastExecutionStart;
    private final Date lastExecutionEnd;
    private final int executionQty;

    public ScheduleExecutionModel(ScheduleId scheduleId, Date lastExecutionStart, Date lastExecutionEnd, int executionQty)
    {
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.lastExecutionStart = Preconditions.checkNotNull(lastExecutionStart, "lastExecution cannot be null");
        this.lastExecutionEnd = Preconditions.checkNotNull(lastExecutionEnd, "lastExecutionEnd cannot be null");
        this.executionQty = executionQty;
    }

    public ScheduleId getScheduleId()
    {
        return scheduleId;
    }

    public Date getLastExecutionStart()
    {
        return lastExecutionStart;
    }

    public Date getLastExecutionEnd()
    {
        return lastExecutionEnd;
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
        if ( !lastExecutionEnd.equals(that.lastExecutionEnd) )
        {
            return false;
        }
        if ( !lastExecutionStart.equals(that.lastExecutionStart) )
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
        result = 31 * result + lastExecutionStart.hashCode();
        result = 31 * result + lastExecutionEnd.hashCode();
        result = 31 * result + executionQty;
        return result;
    }
}
