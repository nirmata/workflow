package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import java.util.Date;

public class StartedTaskModel
{
    private final Date startDateUtc;

    public StartedTaskModel(Date startDateUtc)
    {
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
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
        return startDateUtc.hashCode();
    }

    @Override
    public String toString()
    {
        return "StartedTaskModel{" +
            "startDateUtc=" + startDateUtc +
            '}';
    }
}
