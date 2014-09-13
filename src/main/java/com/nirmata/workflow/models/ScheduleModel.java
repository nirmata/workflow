package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.Clock;
import java.util.Date;

public class ScheduleModel
{
    private final RepetitionModel repetition;
    private final ScheduleId scheduleId;
    private final WorkflowId workflowId;

    public ScheduleModel(ScheduleId scheduleId, WorkflowId workflowId)
    {
        this(scheduleId, workflowId, RepetitionModel.ONCE);
    }

    public ScheduleModel(ScheduleId scheduleId, WorkflowId workflowId, RepetitionModel repetition)
    {
        this.workflowId = Preconditions.checkNotNull(workflowId, "workflowId cannot be null");
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.repetition = Preconditions.checkNotNull(repetition, "repetition cannot be null");
    }

    public RepetitionModel getRepetition()
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

    public boolean shouldExecuteNow(ScheduleExecutionModel scheduleExecution)
    {
        if ( (scheduleExecution.getExecutionQty() + 1) > repetition.getQty() )
        {
            return false;
        }

        Date previousDateUtc = (repetition.getType() == RepetitionModel.Type.ABSOLUTE) ? scheduleExecution.getLastExecutionStartUtc() : scheduleExecution.getLastExecutionEndUtc();
        Date nextDateUtc = new Date(repetition.getDuration().toMillis() + previousDateUtc.getTime());
        return Clock.nowUtc().getTime() > nextDateUtc.getTime();
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

    @Override
    public String toString()
    {
        return "ScheduleModel{" +
            "repetition=" + repetition +
            ", scheduleId=" + scheduleId +
            ", workflowId=" + workflowId +
            '}';
    }
}
