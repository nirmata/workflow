package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;

public class TaskModel
{
    private final TaskId taskId;
    private final String name;
    private final Map<String, String> metaData;
    private final String taskExecutionCode;

    public TaskModel(TaskId taskId, String name, String taskExecutionCode)
    {
        this(taskId, name, taskExecutionCode, Maps.<String, String>newHashMap());
    }

    public TaskModel(TaskId taskId, String name, String taskExecutionCode, Map<String, String> metaData)
    {
        this.taskExecutionCode = Preconditions.checkNotNull(taskExecutionCode, "taskExecutionCode cannot be null");
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");
        this.metaData = ImmutableMap.copyOf(metaData);
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
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
        return result;
    }
}
