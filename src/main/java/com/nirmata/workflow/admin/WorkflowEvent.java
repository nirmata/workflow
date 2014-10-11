package com.nirmata.workflow.admin;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import java.util.Optional;

/**
 * Models a workflow event
 */
public class WorkflowEvent
{
    public enum EventType
    {
        /**
         * A run has started. {@link WorkflowEvent#getRunId()} is the run id.
         */
        RUN_STARTED,

        /**
         * A run has been updated - usually meaning it has completed.
         * {@link WorkflowEvent#getRunId()} is the run id.
         */
        RUN_UPDATED,

        /**
         * A task has started. {@link WorkflowEvent#getRunId()} is the run id.
         * {@link WorkflowEvent#getTaskId()} is the task id.
         */
        TASK_STARTED,

        /**
         * A task has completed. {@link WorkflowEvent#getRunId()} is the run id.
         * {@link WorkflowEvent#getTaskId()} is the task id.
         */
        TASK_COMPLETED
    }

    private final EventType type;
    private final RunId runId;
    private final Optional<TaskId> taskId;

    public WorkflowEvent(EventType type, RunId runId)
    {
        this(type, runId, null);
    }

    public WorkflowEvent(EventType type, RunId runId, TaskId taskId)
    {
        this.type = type;
        this.runId = runId;
        this.taskId = Optional.ofNullable(taskId);
    }

    public EventType getType()
    {
        return type;
    }

    public RunId getRunId()
    {
        return runId;
    }

    public Optional<TaskId> getTaskId()
    {
        return taskId;
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

        WorkflowEvent that = (WorkflowEvent)o;

        if ( !runId.equals(that.runId) )
        {
            return false;
        }
        if ( !taskId.equals(that.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( type != that.type )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + runId.hashCode();
        result = 31 * result + taskId.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "WorkflowEvent{" +
            "type=" + type +
            ", runId=" + runId +
            ", taskId=" + taskId +
            '}';
    }
}
