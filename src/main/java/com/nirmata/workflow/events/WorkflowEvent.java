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
package com.nirmata.workflow.events;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;

/**
 * Models a workflow event
 */
public class WorkflowEvent
{
    public enum EventType
    {
        /**
         * A run has started. {@link WorkflowEvent#getRunId()} is the run id.
         */
        RUN_STARTED,

        /**
         * A run has been updated - usually meaning it has completed.
         * {@link WorkflowEvent#getRunId()} is the run id.
         */
        RUN_UPDATED,

        /**
         * A task has started. {@link WorkflowEvent#getRunId()} is the run id.
         * {@link WorkflowEvent#getTaskId()} is the task id.
         */
        TASK_STARTED,

        /**
         * A task has completed. {@link WorkflowEvent#getRunId()} is the run id.
         * {@link WorkflowEvent#getTaskId()} is the task id.
         */
        TASK_COMPLETED
    }

    private final EventType type;
    private final RunId runId;
    private final TaskId taskId;

    public WorkflowEvent(EventType type, RunId runId)
    {
        this(type, runId, null);
    }

    public WorkflowEvent(EventType type, RunId runId, TaskId taskId)
    {
        this.type = type;
        this.runId = runId;
        this.taskId = taskId;
    }

    public EventType getType()
    {
        return type;
    }

    public RunId getRunId()
    {
        return runId;
    }

    /**
     * @return task id or null if not started.
     */
    public TaskId getTaskId()
    {
        return taskId;
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

        WorkflowEvent that = (WorkflowEvent)o;

        if ( !runId.equals(that.runId) )
        {
            return false;
        }
        if ( taskId == null )
        {
            if ( that.taskId != null )
            {
                return false;
            }
        }
        else if ( !taskId.equals(that.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( type != that.type )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + runId.hashCode();
        result = 31 * result + (taskId == null ? 0 : taskId.hashCode());
        return result;
    }

    @Override
    public String toString()
    {
        return "WorkflowEvent{" +
            "type=" + type +
            ", runId=" + runId +
            ", taskId=" + taskId +
            '}';
    }
}
