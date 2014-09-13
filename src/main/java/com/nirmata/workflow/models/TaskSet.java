package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;

public class TaskSet implements Iterable<TaskId>
{
    private final List<TaskId> tasks;

    public TaskSet(List<TaskId> tasks)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        this.tasks = ImmutableList.copyOf(tasks);
    }

    @Override
    public Iterator<TaskId> iterator()
    {
        return tasks.iterator();
    }

    public int size()
    {
        return tasks.size();
    }

    public TaskId get(int n)
    {
        return tasks.get(n);
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

        TaskSet taskIds = (TaskSet)o;

        //noinspection RedundantIfStatement
        if ( !tasks.equals(taskIds.tasks) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return tasks.hashCode();
    }
}
