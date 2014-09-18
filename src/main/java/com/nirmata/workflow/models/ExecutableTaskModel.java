package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;

public class ExecutableTaskModel
{
    private final ScheduleId scheduleId;
    private final TaskModel task;

    public ExecutableTaskModel(ScheduleId scheduleId, TaskModel task)
    {
        this.scheduleId = Preconditions.checkNotNull(scheduleId, "scheduleId cannot be null");
        this.task = Preconditions.checkNotNull(task, "task cannot be null");
    }

    public ScheduleId getScheduleId()
    {
        return scheduleId;
    }

    public TaskModel getTask()
    {
        return task;
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

        ExecutableTaskModel that = (ExecutableTaskModel)o;

        if ( !scheduleId.equals(that.scheduleId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !task.equals(that.task) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = scheduleId.hashCode();
        result = 31 * result + task.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ExecutableTaskModel{" +
            "scheduleId=" + scheduleId +
            ", task=" + task +
            '}';
    }
}
