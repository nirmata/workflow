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
package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import org.joda.time.LocalDateTime;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RunnableTask implements Serializable
{
    private final Map<TaskId, ExecutableTask> tasks;
    private final List<RunnableTaskDag> taskDags;
    private final LocalDateTime startTimeUtc;
    private final LocalDateTime completionTimeUtc;
    private final RunId parentRunId;

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTimeUtc)
    {
        this(tasks, taskDags, startTimeUtc, null, null);
    }

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTimeUtc, LocalDateTime completionTimeUtc)
    {
        this(tasks, taskDags, startTimeUtc, completionTimeUtc, null);
    }

    public RunnableTask(Map<TaskId, ExecutableTask> tasks, List<RunnableTaskDag> taskDags, LocalDateTime startTimeUtc, LocalDateTime completionTimeUtc, RunId parentRunId)
    {
        this.startTimeUtc = Preconditions.checkNotNull(startTimeUtc, "startTimeUtc cannot be null");
        this.completionTimeUtc = completionTimeUtc;
        this.parentRunId = parentRunId;
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

    /**
     * @return time of completion or null if not yet completed.
     */
    public LocalDateTime getCompletionTimeUtc()
    {
        return completionTimeUtc;
    }

    public LocalDateTime getStartTimeUtc()
    {
        return startTimeUtc;
    }

    /**
     * @return id of parent or null, if this isn't a child task.
     */
    public RunId getParentRunId()
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

        if ( completionTimeUtc == null )
        {
            if ( that.completionTimeUtc != null )
            {
                return false;
            }
        }
        else if ( !completionTimeUtc.equals(that.completionTimeUtc) )
        {
            return false;
        }
        if ( parentRunId == null )
        {
            if ( that.parentRunId != null )
            {
                return false;
            }
        }
        else if ( !parentRunId.equals(that.parentRunId) )
        {
            return false;
        }
        if ( !startTimeUtc.equals(that.startTimeUtc) )
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
        result = 31 * result + startTimeUtc.hashCode();
        result = 31 * result + (completionTimeUtc == null ? 0 : completionTimeUtc.hashCode());
        result = 31 * result + (parentRunId == null ? 0 : parentRunId.hashCode());
        return result;
    }

    @Override
    public String toString()
    {
        return "RunnableTask{" +
            "tasks=" + tasks +
            ", taskDags=" + taskDags +
            ", startTime=" + startTimeUtc +
            ", completionTime=" + completionTimeUtc +
            ", parentRunId=" + parentRunId +
            '}';
    }
}
