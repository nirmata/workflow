package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.ScheduleExecutionModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.TaskDagModel;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.TaskSets;
import com.nirmata.workflow.models.WorkflowId;
import java.time.LocalDateTime;
import java.util.List;

public class DenormalizedWorkflowModel
{
    private final RunId runId;
    private final ScheduleExecutionModel scheduleExecution;
    private final WorkflowId workflowId;
    private final List<TaskModel> tasks;
    private final String name;
    private final TaskSets taskSets;
    private final int taskSetsIndex;
    private final TaskDagModel taskDag;
    private final LocalDateTime startDateUtc;

    public DenormalizedWorkflowModel(RunId runId, ScheduleExecutionModel scheduleExecution, WorkflowId workflowId, List<TaskModel> tasks, String name, TaskDagModel taskDag, TaskSets taskSets, LocalDateTime startDateUtc, int taskSetsIndex)
    {
        this.taskDag = Preconditions.checkNotNull(taskDag, "taskDag cannot be null");
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        this.scheduleExecution = Preconditions.checkNotNull(scheduleExecution, "scheduleExecution cannot be null");
        this.workflowId = Preconditions.checkNotNull(workflowId, "workflowId cannot be null");
        this.taskSetsIndex = taskSetsIndex;
        this.tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.taskSets = Preconditions.checkNotNull(taskSets, "taskSets cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
    }

    public TaskDagModel getTaskDag()
    {
        return taskDag;
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

    public List<TaskModel> getTasks()
    {
        return tasks;
    }

    public String getName()
    {
        return name;
    }

    public TaskSets getTaskSets()
    {
        return taskSets;
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
    }

    public int getTaskSetsIndex()
    {
        return taskSetsIndex;
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

        if ( taskSetsIndex != that.taskSetsIndex )
        {
            return false;
        }
        if ( !name.equals(that.name) )
        {
            return false;
        }
        if ( !runId.equals(that.runId) )
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
        if ( !taskDag.equals(that.taskDag) )
        {
            return false;
        }
        if ( !taskSets.equals(that.taskSets) )
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
        result = 31 * result + scheduleExecution.hashCode();
        result = 31 * result + workflowId.hashCode();
        result = 31 * result + tasks.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + taskSets.hashCode();
        result = 31 * result + taskSetsIndex;
        result = 31 * result + taskDag.hashCode();
        result = 31 * result + startDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "DenormalizedWorkflowModel{" +
            "runId=" + runId +
            ", scheduleExecution=" + scheduleExecution +
            ", workflowId=" + workflowId +
            ", tasks=" + tasks +
            ", name='" + name + '\'' +
            ", taskSets=" + taskSets +
            ", taskSetsIndex=" + taskSetsIndex +
            ", taskDag=" + taskDag +
            ", startDateUtc=" + startDateUtc +
            '}';
    }
}
