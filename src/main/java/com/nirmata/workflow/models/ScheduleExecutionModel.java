package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.util.Date;

public class ScheduleExecutionModel
{
    private final ScheduleId scheduleId;
    private final Date lastExecution;
    private final int executionQty;

    public ScheduleExecutionModel(ScheduleId scheduleId, Date lastExecution, int executionQty)
    {
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.lastExecution = Preconditions.checkNotNull(lastExecution, "lastExecution cannot be null");
        this.executionQty = executionQty;
    }

    public ScheduleId getScheduleId()
    {
        return scheduleId;
    }

    public Date getLastExecution()
    {
        return lastExecution;
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
        if ( !lastExecution.equals(that.lastExecution) )
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
        result = 31 * result + lastExecution.hashCode();
        result = 31 * result + executionQty;
        return result;
    }
}
