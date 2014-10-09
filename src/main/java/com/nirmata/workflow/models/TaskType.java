package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class TaskType
{
    private final String type;
    private final boolean isIdempotent;

    public TaskType(String type, boolean isIdempotent)
    {
        this.isIdempotent = isIdempotent;
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
    }

    public String getType()
    {
        return type;
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

        TaskType taskType = (TaskType)o;

        if ( isIdempotent != taskType.isIdempotent )
        {
            return false;
        }
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
        int result = type.hashCode();
        result = 31 * result + (isIdempotent ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskType{" +
            "type='" + type + '\'' +
            ", isIdempotent=" + isIdempotent +
            '}';
    }
}
