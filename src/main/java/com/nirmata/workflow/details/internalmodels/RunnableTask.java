package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskId;
import java.util.List;
import java.util.Map;

public class RunnableTask
{
    private final Map<TaskId, ExecutableTask> tasks;
    private final List<RunnableTaskDag> taskDags;

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags)
    {
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        taskDags = Preconditions.checkNotNull(taskDags, "taskDags cannot be null");

        this.tasks = ImmutableMap.copyOf(tasks);
        this.taskDags = ImmutableList.copyOf(taskDags);
    }

    public Map<TaskId, ExecutableTask> getTasks()
    {
        return tasks;
    }

    public List<RunnableTaskDag> getTaskDags()
    {
        return taskDags;
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

        RunnableTask that = (RunnableTask)o;

        if ( !taskDags.equals(that.taskDags) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !tasks.equals(that.tasks) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = tasks.hashCode();
        result = 31 * result + taskDags.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunnableTask{" +
            "tasks=" + tasks +
            ", taskDags=" + taskDags +
            '}';
    }
}
