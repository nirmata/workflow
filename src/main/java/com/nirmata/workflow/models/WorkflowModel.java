package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class WorkflowModel
{
    private final WorkflowId workflowId;
    private final String name;
    private final TaskSet tasks;

    public WorkflowModel(WorkflowId workflowId, String name, TaskSet tasks)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.tasks = tasks;
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

    public TaskSet getTasks()
    {
        return tasks;
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
        result = 31 * result + name.hashCode();
        result = 31 * result + tasks.hashCode();
        return result;
    }
}
