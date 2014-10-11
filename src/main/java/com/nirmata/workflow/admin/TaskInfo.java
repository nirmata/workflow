package com.nirmata.workflow.admin;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import java.time.LocalDateTime;
import java.util.Optional;

public class TaskInfo
{
    private final TaskId taskId;
    private final String instanceName;
    private final LocalDateTime startDateUtc;
    private final Optional<TaskExecutionResult> result;

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc)
    {
        this(taskId, instanceName, startDateUtc, null);
    }

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc, TaskExecutionResult result)
    {
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
        this.result = Optional.ofNullable(result);
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
    }

    public TaskExecutionResult getResult()
    {
        return result.get();
    }

    public boolean isComplete()
    {
        return result.isPresent();
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

        TaskInfo taskInfo = (TaskInfo)o;

        if ( !instanceName.equals(taskInfo.instanceName) )
        {
            return false;
        }
        if ( !result.equals(taskInfo.result) )
        {
            return false;
        }
        if ( !startDateUtc.equals(taskInfo.startDateUtc) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskId.equals(taskInfo.taskId) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result1 = taskId.hashCode();
        result1 = 31 * result1 + instanceName.hashCode();
        result1 = 31 * result1 + startDateUtc.hashCode();
        result1 = 31 * result1 + result.hashCode();
        return result1;
    }

    @Override
    public String toString()
    {
        return "TaskInfo{" +
            "taskId=" + taskId +
            ", instanceName='" + instanceName + '\'' +
            ", startDateUtc=" + startDateUtc +
            ", result=" + result +
            '}';
    }
}
