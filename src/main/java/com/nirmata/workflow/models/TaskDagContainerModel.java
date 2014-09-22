package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class TaskDagContainerModel
{
    private final TaskDagId taskDagId;
    private final TaskDagModel dag;

    public TaskDagContainerModel(TaskDagId taskDagId, TaskDagModel dag)
    {
        this.taskDagId = Preconditions.checkNotNull(taskDagId, "dagId cannot be null");
        this.dag = Preconditions.checkNotNull(dag, "dag cannot be null");
    }

    public TaskDagId getTaskDagId()
    {
        return taskDagId;
    }

    public TaskDagModel getDag()
    {
        return dag;
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

        TaskDagContainerModel that = (TaskDagContainerModel)o;

        if ( !dag.equals(that.dag) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskDagId.equals(that.taskDagId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = taskDagId.hashCode();
        result = 31 * result + dag.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskDagContainerModel{" +
            "dagId=" + taskDagId +
            ", dag=" + dag +
            '}';
    }
}
