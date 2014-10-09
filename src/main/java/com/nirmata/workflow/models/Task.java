package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;

public class Task
{
    private final TaskId taskId;
    private final TaskType taskType;
    private final boolean isIdempotent;
    private final List<Task> childrenTasks;

    public Task(TaskId taskId, TaskType taskType, boolean isIdempotent)
    {
        this(taskId, taskType, isIdempotent, Lists.newArrayList());
    }

    public Task(TaskId taskId, TaskType taskType, boolean isIdempotent, List<Task> childrenTasks)
    {
        childrenTasks = Preconditions.checkNotNull(childrenTasks, "childrenTasks cannot be null");
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.taskType = Preconditions.checkNotNull(taskType, "taskType cannot be null");
        this.isIdempotent = isIdempotent;

        this.childrenTasks = ImmutableList.copyOf(childrenTasks);
    }

    public List<Task> getChildrenTasks()
    {
        return childrenTasks;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskType getTaskType()
    {
        return taskType;
    }

    public boolean isIdempotent()
    {
        return isIdempotent;
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

        Task task = (Task)o;

        if ( isIdempotent != task.isIdempotent )
        {
            return false;
        }
        if ( !childrenTasks.equals(task.childrenTasks) )
        {
            return false;
        }
        if ( !taskId.equals(task.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskType.equals(task.taskType) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = taskId.hashCode();
        result = 31 * result + taskType.hashCode();
        result = 31 * result + (isIdempotent ? 1 : 0);
        result = 31 * result + childrenTasks.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Task{" +
            "taskId=" + taskId +
            ", taskType=" + taskType +
            ", isIdempotent=" + isIdempotent +
            ", childrenTasks=" + childrenTasks +
            '}';
    }
}
