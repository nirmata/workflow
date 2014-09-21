package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class RunnableTaskDagModel
{
    private final List<RunnableTaskDagEntryModel> entries;

    public RunnableTaskDagModel(List<RunnableTaskDagEntryModel> entries)
    {
        entries = Preconditions.checkNotNull(entries, "entries cannot be null");
        this.entries = ImmutableList.copyOf(entries);
    }

    public List<RunnableTaskDagEntryModel> getEntries()
    {
        return entries;
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

        RunnableTaskDagModel that = (RunnableTaskDagModel)o;

        //noinspection RedundantIfStatement
        if ( !entries.equals(that.entries) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return entries.hashCode();
    }

    @Override
    public String toString()
    {
        return "RunnableTaskDagModel{" +
            "entries=" + entries +
            '}';
    }
}
