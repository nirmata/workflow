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
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import java.util.Map;
import java.util.Optional;

/**
 * Contains the meta-data and type from a submitted {@link Task}
 */
public class TaskDetails
{
    private final TaskId taskId;
    private final Optional<TaskType> taskType;
    private final Map<String, String> metaData;

    public TaskDetails(TaskId taskId, TaskType taskType, Map<String, String> metaData)
    {
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.taskType = Optional.ofNullable(taskType);
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");

        this.metaData = ImmutableMap.copyOf(metaData);
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public boolean isExecutable()
    {
        return taskType.isPresent();
    }

    public TaskType getTaskType()
    {
        //noinspection ConstantConditions
        return taskType.get(); // exception if empty is desired
    }

    public Map<String, String> getMetaData()
    {
        return metaData;
    }

    public boolean matchesTask(Task task)
    {
        if ( task == null )
        {
            return false;
        }
        TaskDetails rhs = new TaskDetails(task.getTaskId(), task.isExecutable() ? task.getTaskType() : null, task.getMetaData());
        return this.equals(rhs);
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

        TaskDetails that = (TaskDetails)o;

        if ( !metaData.equals(that.metaData) )
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
        int result = taskId.hashCode();
        result = 31 * result + taskType.hashCode();
        result = 31 * result + metaData.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "TaskDetails{" +
            "taskId=" + taskId +
            ", taskType=" + taskType +
            ", metaData=" + metaData +
            '}';
    }
}
