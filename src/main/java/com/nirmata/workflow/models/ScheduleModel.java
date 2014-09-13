package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class ScheduleModel
{
    private final Repetition repetition;
    private final ScheduleId scheduleId;
    private final WorkflowId workflowId;

    public ScheduleModel(ScheduleId scheduleId, WorkflowId workflowId)
    {
        this(scheduleId, workflowId, Repetition.NONE);
    }

    public ScheduleModel(ScheduleId scheduleId, WorkflowId workflowId, Repetition repetition)
    {
        this.workflowId = Preconditions.checkNotNull(workflowId, "workflowId cannot be null");
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.repetition = Preconditions.checkNotNull(repetition, "repetition cannot be null");
    }

    public Repetition getRepetition()
    {
        return repetition;
    }

    public ScheduleId getScheduleId()
    {
        return scheduleId;
    }

    public WorkflowId getWorkflowId()
    {
        return workflowId;
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

        ScheduleModel that = (ScheduleModel)o;

        if ( !repetition.equals(that.repetition) )
        {
            return false;
        }
        if ( !scheduleId.equals(that.scheduleId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !workflowId.equals(that.workflowId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = repetition.hashCode();
        result = 31 * result + scheduleId.hashCode();
        result = 31 * result + workflowId.hashCode();
        return result;
    }
}
