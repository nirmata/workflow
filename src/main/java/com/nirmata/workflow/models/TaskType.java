package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class TaskType
{
    private final String type;

    public TaskType(String type)
    {
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
    }

    public String getType()
    {
        return type;
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

        TaskType taskType = (TaskType)o;

        //noinspection RedundantIfStatement
        if ( !type.equals(taskType.type) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode();
    }

    @Override
    public String toString()
    {
        return "TaskType{" +
            "type='" + type + '\'' +
            '}';
    }
}
