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
import org.joda.time.LocalDateTime;

/**
 * Task information
 */
public class TaskInfo
{
    private final TaskId taskId;
    private final String instanceName;
    private final LocalDateTime startDateUtc;
    private final TaskExecutionResult result;

    public TaskInfo(TaskId taskId)
    {
        this(taskId, null, null, null);
    }

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc)
    {
        this(taskId, instanceName, startDateUtc, null);
    }

    public TaskInfo(TaskId taskId, String instanceName, LocalDateTime startDateUtc, TaskExecutionResult result)
    {
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.instanceName = instanceName;
        this.startDateUtc = startDateUtc;
        this.result = result;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    /**
     * @return instance name or null if not started.
     */
    public String getInstanceName()
    {
        return instanceName;
    }

    /**
     * @return start data name or null if not started.
     */
    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
    }

    /**
     * @return result or null if not complete
     */
    public TaskExecutionResult getResult()
    {
        return result;
    }

    public boolean hasStarted()
    {
        return startDateUtc != null && instanceName != null;
    }

    public boolean isComplete()
    {
        return result != null;
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

        if ( instanceName == null )
        {
            if ( taskInfo.instanceName != null )
            {
                return false;
            }
        }
        else if ( !instanceName.equals(taskInfo.instanceName) )
        {
            return false;
        }
        if ( result == null )
        {
            if ( taskInfo.result != null )
            {
                return false;
            }
        }
        else if ( !result.equals(taskInfo.result) )
        {
            return false;
        }
        if ( startDateUtc == null )
        {
            if ( taskInfo.startDateUtc != null )
            {
                return false;
            }
        }
        else if ( !startDateUtc.equals(taskInfo.startDateUtc) )
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
        result1 = 31 * result1 + (instanceName == null ? 0 : instanceName.hashCode());
        result1 = 31 * result1 + (startDateUtc == null ? 0 : startDateUtc.hashCode());
        result1 = 31 * result1 + (result == null ? 0 : result.hashCode());
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
