package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class TaskDagContainerModel
{
    private final DagId dagId;
    private final TaskDagModel dag;

    public TaskDagContainerModel(DagId dagId, TaskDagModel dag)
    {
        this.dagId = Preconditions.checkNotNull(dagId, "dagId cannot be null");
        this.dag = Preconditions.checkNotNull(dag, "dag cannot be null");
    }

    public DagId getDagId()
    {
        return dagId;
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
        if ( !dagId.equals(that.dagId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = dagId.hashCode();
        result = 31 * result + dag.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskDagContainerModel{" +
            "dagId=" + dagId +
            ", dag=" + dag +
            '}';
    }
}
