package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;

public class TaskSets implements Iterable<List<TaskId>>
{
    private final List<List<TaskId>> tasks;

    public TaskSets(List<List<TaskId>> tasks)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        ImmutableList.Builder<List<TaskId>> builder = ImmutableList.builder();
        for ( List<TaskId> l : tasks )
        {
            builder.add(ImmutableList.copyOf(l));
        }
        this.tasks = builder.build();
    }

    @Override
    public Iterator<List<TaskId>> iterator()
    {
        return tasks.iterator();
    }

    public int size()
    {
        return tasks.size();
    }

    public List<TaskId> get(int n)
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

        TaskSets taskIds = (TaskSets)o;

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

    @Override
    public String toString()
    {
        return "TaskSet{" +
            "tasks=" + tasks +
            '}';
    }
}
