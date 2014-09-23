package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.details.WorkflowStatus;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.WorkflowId;
import java.time.LocalDateTime;
import java.util.Map;

public class DenormalizedWorkflowModel
{
    private final RunId runId;
    private final WorkflowStatus status;
    private final ScheduleExecutionModel scheduleExecution;
    private final WorkflowId workflowId;
    private final Map<TaskId, TaskModel> tasks;
    private final String name;
    private final RunnableTaskDagModel runnableTaskDag;
    private final LocalDateTime startDateUtc;

    public DenormalizedWorkflowModel(RunId runId, WorkflowStatus status, ScheduleExecutionModel scheduleExecution, WorkflowId workflowId, Map<TaskId, TaskModel> tasks, String name, RunnableTaskDagModel runnableTaskDag, LocalDateTime startDateUtc)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.status = Preconditions.checkNotNull(status, "status cannot be null");
        this.runnableTaskDag = Preconditions.checkNotNull(runnableTaskDag, "runnableTaskDag cannot be null");
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        this.scheduleExecution = Preconditions.checkNotNull(scheduleExecution, "scheduleExecution cannot be null");
        this.workflowId = Preconditions.checkNotNull(workflowId, "workflowId cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
        this.tasks = ImmutableMap.copyOf(tasks);
    }

    public RunnableTaskDagModel getRunnableTaskDag()
    {
        return runnableTaskDag;
    }

    public WorkflowStatus getStatus()
    {
        return status;
    }

    public RunId getRunId()
    {
        return runId;
    }

    public ScheduleId getScheduleId()
    {
        return scheduleExecution.getScheduleId();
    }

    public ScheduleExecutionModel getScheduleExecution()
    {
        return scheduleExecution;
    }

    public WorkflowId getWorkflowId()
    {
        return workflowId;
    }

    public Map<TaskId, TaskModel> getTasks()
    {
        return tasks;
    }

    public String getName()
    {
        return name;
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
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

        DenormalizedWorkflowModel that = (DenormalizedWorkflowModel)o;

        if ( !name.equals(that.name) )
        {
            return false;
        }
        if ( !runId.equals(that.runId) )
        {
            return false;
        }
        if ( !runnableTaskDag.equals(that.runnableTaskDag) )
        {
            return false;
        }
        if ( !scheduleExecution.equals(that.scheduleExecution) )
        {
            return false;
        }
        if ( !startDateUtc.equals(that.startDateUtc) )
        {
            return false;
        }
        if ( status != that.status )
        {
            return false;
        }
        if ( !tasks.equals(that.tasks) )
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
        int result = runId.hashCode();
        result = 31 * result + status.hashCode();
        result = 31 * result + scheduleExecution.hashCode();
        result = 31 * result + workflowId.hashCode();
        result = 31 * result + tasks.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + runnableTaskDag.hashCode();
        result = 31 * result + startDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "DenormalizedWorkflowModel{" +
            "runId=" + runId +
            ", status=" + status +
            ", scheduleExecution=" + scheduleExecution +
            ", workflowId=" + workflowId +
            ", tasks=" + tasks +
            ", name='" + name + '\'' +
            ", runnableTaskDag=" + runnableTaskDag +
            ", startDateUtc=" + startDateUtc +
            '}';
    }
}
