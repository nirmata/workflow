package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.util.Date;

public class StartedTaskModel
{
    private final String instanceName;
    private final Date startDateUtc;

    public StartedTaskModel(String instanceName, Date startDateUtc)
    {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    public Date getStartDateUtc()
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

        StartedTaskModel that = (StartedTaskModel)o;

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
