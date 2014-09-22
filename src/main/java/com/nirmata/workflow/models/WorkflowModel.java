package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

/**
 * Models an entire workflow
 */
public class WorkflowModel
{
    private final WorkflowId workflowId;
    private final String name;
    private final TaskDagId taskDagId;

    /**
     * @param workflowId the workflow Id
     * @param name workflow name (for display only)
     * @param taskDagId the dag to use
     */
    public WorkflowModel(WorkflowId workflowId, String name, TaskDagId taskDagId)
    {
        this.taskDagId = Preconditions.checkNotNull(taskDagId, "taskDagId cannot be null");
        this.workflowId = Preconditions.checkNotNull(workflowId, "workflowId cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
    }

    public WorkflowId getWorkflowId()
    {
        return workflowId;
    }

    public String getName()
    {
        return name;
    }

    public TaskDagId getTaskDagId()
    {
        return taskDagId;
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

        WorkflowModel that = (WorkflowModel)o;

        if ( !name.equals(that.name) )
        {
            return false;
        }
        if ( !taskDagId.equals(that.taskDagId) )
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
        result = 31 * result + name.hashCode();
        result = 31 * result + taskDagId.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "WorkflowModel{" +
            "workflowId=" + workflowId +
            ", name='" + name + '\'' +
            ", taskDagId=" + taskDagId +
            '}';
    }
}
