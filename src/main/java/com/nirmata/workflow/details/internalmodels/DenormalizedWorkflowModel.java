package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.TaskModel;
import com.nirmata.workflow.models.TaskSets;
import com.nirmata.workflow.models.WorkflowId;
import java.util.Date;
import java.util.List;

public class DenormalizedWorkflowModel
{
    private final WorkflowId workflowId;
    private final List<TaskModel> tasks;
    private final String name;
    private final TaskSets taskSets;
    private final Date startDateUtc;

    public DenormalizedWorkflowModel(WorkflowId workflowId, List<TaskModel> tasks, String name, TaskSets taskSets, Date startDateUtc)
    {
        this.workflowId = workflowId;
        this.tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.taskSets = Preconditions.checkNotNull(taskSets, "taskSets cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
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

    public Date getStartDateUtc()
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
        if ( !startDateUtc.equals(that.startDateUtc) )
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
        int result = workflowId.hashCode();
        result = 31 * result + tasks.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + taskSets.hashCode();
        result = 31 * result + startDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "DenormalizedWorkflowModel{" +
            "workflowId=" + workflowId +
            ", tasks=" + tasks +
            ", name='" + name + '\'' +
            ", taskSets=" + taskSets +
            ", startDateUtc=" + startDateUtc +
            '}';
    }
}
