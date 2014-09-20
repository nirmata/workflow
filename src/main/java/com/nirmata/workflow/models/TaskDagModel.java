package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class TaskDagModel
{
    private final List<TaskId> tasks;
    private final List<TaskDagModel> siblings;
    private final List<TaskDagModel> children;

    public TaskDagModel(List<TaskId> tasks, List<TaskDagModel> siblings, List<TaskDagModel> children)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        siblings = Preconditions.checkNotNull(siblings, "siblings cannot be null");
        children = Preconditions.checkNotNull(children, "children cannot be null");
        this.tasks = ImmutableList.copyOf(tasks);
        this.siblings = ImmutableList.copyOf(siblings);
        this.children = ImmutableList.copyOf(children);
    }

    public List<TaskId> getTasks()
    {
        return tasks;
    }

    public List<TaskDagModel> getSiblings()
    {
        return siblings;
    }

    public List<TaskDagModel> getChildren()
    {
        return children;
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

        TaskDagModel taskDag = (TaskDagModel)o;

        if ( !children.equals(taskDag.children) )
        {
            return false;
        }
        if ( !siblings.equals(taskDag.siblings) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !tasks.equals(taskDag.tasks) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = tasks.hashCode();
        result = 31 * result + siblings.hashCode();
        result = 31 * result + children.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskDag{" +
            "tasks=" + tasks +
            ", siblings=" + siblings +
            ", children=" + children +
            '}';
    }
}
