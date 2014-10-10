package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class ExecutableTask
{
    private final TaskId taskId;
    private final TaskType taskType;
    private final Map<String, String> metaData;

    public ExecutableTask(TaskId taskId, TaskType taskType, Map<String, String> metaData)
    {
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.taskType = Preconditions.checkNotNull(taskType, "taskType cannot be null");
        this.metaData = ImmutableMap.copyOf(metaData);
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskType getTaskType()
    {
        return taskType;
    }

    public Map<String, String> getMetaData()
    {
        return metaData;
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

        ExecutableTask that = (ExecutableTask)o;

        if ( !metaData.equals(that.metaData) )
        {
            return false;
        }
        if ( !taskId.equals(that.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskType.equals(that.taskType) )
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
        result = 31 * result + metaData.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ExecutableTask{" +
            "taskId=" + taskId +
            ", taskType=" + taskType +
            ", metaData=" + metaData +
            '}';
    }
}
