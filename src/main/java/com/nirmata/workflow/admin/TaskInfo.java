/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.admin;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Task information
 */
@SuppressWarnings("ConstantConditions")  // exception if empty is desired
public class TaskInfo
{
    private final TaskId taskId;
    private final Optional<String> instanceName;
    private final Optional<LocalDateTime> startDateUtc;
    private final Optional<Integer> progress;
    private final Optional<TaskExecutionResult> result;

    public TaskInfo(TaskId taskId)
    {
        this(taskId, null, null, 0, null);
    }

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc, int progress)
    {
        this(taskId, instanceName, startDateUtc, progress, null);
    }

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc, int progress, TaskExecutionResult result)
    {
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.instanceName = Optional.ofNullable(instanceName);
        this.startDateUtc = Optional.ofNullable(startDateUtc);
        this.result = Optional.ofNullable(result);
        this.progress = Optional.of(progress);
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public String getInstanceName()
    {
        return instanceName.get();
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc.get();
    }

    public TaskExecutionResult getResult()
    {
        return result.get();
    }
    
    public int getProgress() 
    {
        return progress.get();
    }

    public boolean hasStarted()
    {
        return startDateUtc.isPresent() && instanceName.isPresent();
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
            ", progress=" + progress +
            ", result=" + result +
            '}';
    }
}
