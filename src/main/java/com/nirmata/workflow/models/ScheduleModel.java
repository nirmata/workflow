package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Models a schedule
 */
public class ScheduleModel
{
    private final RepetitionModel repetition;
    private final ScheduleId scheduleId;
    private final WorkflowId workflowId;

    /**
     * A schedule that executes once
     *
     * @param scheduleId the schedule Id
     * @param workflowId the workflow Id that gets executed
     */
    public ScheduleModel(ScheduleId scheduleId, WorkflowId workflowId)
    {
        this(scheduleId, workflowId, RepetitionModel.ONCE);
    }

    /**
     * @param scheduleId the schedule Id
     * @param workflowId the workflow Id that gets executed
     * @param repetition the repetition model
     */
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

        LocalDateTime previousDateUtc = (repetition.getType() == RepetitionModel.Type.ABSOLUTE) ? scheduleExecution.getLastExecutionStartUtc() : scheduleExecution.getLastExecutionEndUtc();
        LocalDateTime nextDateUtc = previousDateUtc.plus(repetition.getDuration().toMillis(), ChronoUnit.MILLIS);
        return LocalDateTime.now(Clock.systemUTC()).isAfter(nextDateUtc);
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
