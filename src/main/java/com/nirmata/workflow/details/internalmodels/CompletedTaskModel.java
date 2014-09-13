package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class CompletedTaskModel
{
    private final boolean isComplete;
    private final Map<String, String> resultData;

    public CompletedTaskModel()
    {
        isComplete = false;
        resultData = ImmutableMap.of();
    }

    public CompletedTaskModel(boolean isComplete, Map<String, String> resultData)
    {
        this.isComplete = isComplete;
        this.resultData = Preconditions.checkNotNull(resultData, "resultData cannot be null");
    }

    public boolean isComplete()
    {
        return isComplete;
    }

    public Map<String, String> getResultData()
    {
        return resultData;
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

        CompletedTaskModel that = (CompletedTaskModel)o;

        if ( isComplete != that.isComplete )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !resultData.equals(that.resultData) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (isComplete ? 1 : 0);
        result = 31 * result + resultData.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "CompletedTaskModel{" +
            "isComplete=" + isComplete +
            ", resultData=" + resultData +
            '}';
    }
}
