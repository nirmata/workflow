package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RunnableTask
{
    private final Map<TaskId, ExecutableTask> tasks;
    private final List<RunnableTaskDag> taskDags;
    private final LocalDateTime startTime;
    private final Optional<LocalDateTime> completionTime;
    private final Optional<RunId> parentRunId;

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTime)
    {
        this(tasks, taskDags, startTime, null, null);
    }

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTime, LocalDateTime completionTime)
    {
        this(tasks, taskDags, startTime, completionTime, null);
    }

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTime, LocalDateTime completionTime, RunId parentRunId)
    {
        this.startTime = Preconditions.checkNotNull(startTime, "startTime cannot be null");
        this.completionTime = Optional.ofNullable(completionTime);
        this.parentRunId = Optional.ofNullable(parentRunId);
        tasks = Preconditions.checkNotNull(tasks, "tasks cannot be null");
        taskDags = Preconditions.checkNotNull(taskDags, "taskDags cannot be null");

        this.tasks = ImmutableMap.copyOf(tasks);
        this.taskDags = ImmutableList.copyOf(taskDags);
    }

    public Map<TaskId, ExecutableTask> getTasks()
    {
        return tasks;
    }

    public List<RunnableTaskDag> getTaskDags()
    {
        return taskDags;
    }

    public Optional<LocalDateTime> getCompletionTime()
    {
        return completionTime;
    }

    public LocalDateTime getStartTime()
    {
        return startTime;
    }

    public Optional<RunId> getParentRunId()
    {
        return parentRunId;
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

        RunnableTask that = (RunnableTask)o;

        if ( !completionTime.equals(that.completionTime) )
        {
            return false;
        }
        if ( !parentRunId.equals(that.parentRunId) )
        {
            return false;
        }
        if ( !startTime.equals(that.startTime) )
        {
            return false;
        }
        if ( !taskDags.equals(that.taskDags) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !tasks.equals(that.tasks) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = tasks.hashCode();
        result = 31 * result + taskDags.hashCode();
        result = 31 * result + startTime.hashCode();
        result = 31 * result + completionTime.hashCode();
        result = 31 * result + parentRunId.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunnableTask{" +
            "tasks=" + tasks +
            ", taskDags=" + taskDags +
            ", startTime=" + startTime +
            ", completionTime=" + completionTime +
            ", parentRunId=" + parentRunId +
            '}';
    }
}
