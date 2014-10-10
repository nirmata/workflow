package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import java.time.LocalDateTime;

public class StartedTask
{
    private final String instanceName;
    private final LocalDateTime startDateUtc;

    public StartedTask(String instanceName, LocalDateTime startDateUtc)
    {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
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

        StartedTask that = (StartedTask)o;

        if ( !instanceName.equals(that.instanceName) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !startDateUtc.equals(that.startDateUtc) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = instanceName.hashCode();
        result = 31 * result + startDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "StartedTaskModel{" +
            "instanceName='" + instanceName + '\'' +
            ", startDateUtc=" + startDateUtc +
            '}';
    }
}
