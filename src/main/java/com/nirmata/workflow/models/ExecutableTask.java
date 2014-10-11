package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Models a task that has been scheduled for execution
 * as part of a run
 */
public class ExecutableTask
{
    private final RunId runId;
    private final TaskId taskId;
    private final TaskType taskType;
    private final Map<String, String> metaData;
    private final boolean isExecutable;

    /**
     * @param runId the run that this is part of
     * @param taskId the task
     * @param taskType task type
     * @param metaData meta data
     * @param isExecutable if false, this is merely a container for child tasks
     */
    public ExecutableTask(RunId runId, TaskId taskId, TaskType taskType, Map<String, String> metaData, boolean isExecutable)
    {
        this.runId = Preconditions.checkNotNull(runId, "runId cannot be null");
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");
        this.isExecutable = isExecutable;
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.taskType = Preconditions.checkNotNull(taskType, "taskType cannot be null");
        this.metaData = ImmutableMap.copyOf(metaData);
    }

    public RunId getRunId()
    {
        return runId;
    }

    public boolean isExecutable()
    {
        return isExecutable;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskType getTaskType()
    {
        return taskType;
    }

    public Map<String, String> getMetaData()
    {
        return metaData;
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

        ExecutableTask that = (ExecutableTask)o;

        if ( isExecutable != that.isExecutable )
        {
            return false;
        }
        if ( !metaData.equals(that.metaData) )
        {
            return false;
        }
        if ( !runId.equals(that.runId) )
        {
            return false;
        }
        if ( !taskId.equals(that.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskType.equals(that.taskType) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = runId.hashCode();
        result = 31 * result + taskId.hashCode();
        result = 31 * result + taskType.hashCode();
        result = 31 * result + metaData.hashCode();
        result = 31 * result + (isExecutable ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ExecutableTask{" +
            "runId=" + runId +
            ", taskId=" + taskId +
            ", taskType=" + taskType +
            ", metaData=" + metaData +
            ", isExecutable=" + isExecutable +
            '}';
    }
}
