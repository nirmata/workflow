package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 * Models a executable task
 */
public class TaskModel
{
    private final TaskId taskId;
    private final String name;
    private final Map<String, String> metaData;
    private final String taskExecutionCode;
    private final boolean isIdempotent;

    /**
     * @param taskId task Id
     * @param name the task's name (used for display only)
     * @param taskExecutionCode the execution code - whatever is needed to execute the task
     * @param isIdempotent true if the task is idempotent (it will get retried as needed)
     */
    public TaskModel(TaskId taskId, String name, String taskExecutionCode, boolean isIdempotent)
    {
        this(taskId, name, taskExecutionCode, isIdempotent, Maps.<String, String>newHashMap());
    }

    /**
     * @param taskId task Id
     * @param name the task's name (used for display only)
     * @param taskExecutionCode the execution code - whatever is needed to execute the task
     * @param isIdempotent true if the task is idempotent (it will get retried as needed)
     * @param metaData optional metadata for the task
     */
    public TaskModel(TaskId taskId, String name, String taskExecutionCode, boolean isIdempotent, Map<String, String> metaData)
    {
        this.taskExecutionCode = Preconditions.checkNotNull(taskExecutionCode, "taskExecutionCode cannot be null");
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");
        this.metaData = ImmutableMap.copyOf(metaData);
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.isIdempotent = isIdempotent;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getMetaData()
    {
        return metaData;
    }

    public String getTaskExecutionCode()
    {
        return taskExecutionCode;
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

        TaskModel taskModel = (TaskModel)o;

        if ( isIdempotent != taskModel.isIdempotent )
        {
            return false;
        }
        if ( !metaData.equals(taskModel.metaData) )
        {
            return false;
        }
        if ( !name.equals(taskModel.name) )
        {
            return false;
        }
        if ( !taskExecutionCode.equals(taskModel.taskExecutionCode) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskId.equals(taskModel.taskId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = taskId.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + metaData.hashCode();
        result = 31 * result + taskExecutionCode.hashCode();
        result = 31 * result + (isIdempotent ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskModel{" +
            "taskId=" + taskId +
            ", name='" + name + '\'' +
            ", metaData=" + metaData +
            ", taskExecutionCode='" + taskExecutionCode + '\'' +
            ", isIdempotent=" + isIdempotent +
            '}';
    }
}
