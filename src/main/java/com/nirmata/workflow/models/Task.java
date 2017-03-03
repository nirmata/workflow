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
package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Models a task
 */
public class Task implements Serializable
{
    private final TaskId taskId;
    private final Optional<TaskType> taskType;
    private final List<Task> childrenTasks;
    private final Map<String, String> metaData;

    /**
     * Metadata value used by special purpose executors
     */
    public static final String META_TASK_SUBMIT_VALUE = "__submit_value";

    /**
     * Utility to create a new meta map with the given submit value
     *
     * @param value the submit value
     * @return new meta map
     */
    public static Map<String, String> makeSpecialMeta(long value)
    {
        Map<String, String> meta = Maps.newHashMap();
        meta.put(META_TASK_SUBMIT_VALUE, Long.toString(value));
        return meta;
    }

    /**
     * @param taskId this task's ID - must be unique
     * @param taskType the task type
     */
    public Task(TaskId taskId, TaskType taskType)
    {
        this(taskId, taskType, Lists.newArrayList(), Maps.newHashMap());
    }

    /**
     * This constructor makes a "container only" task. i.e. this task does
     * not execute anything. It exists solely to contain children tasks.
     *
     * @param taskId this task's ID - must be unique
     * @param childrenTasks child tasks - children are not executed until this task completes
     */
    public Task(TaskId taskId, List<Task> childrenTasks)
    {
        this(taskId, null, childrenTasks, Maps.newHashMap());
    }

    /**
     * @param taskId this task's ID - must be unique
     * @param taskType the task type
     * @param childrenTasks child tasks - children are not executed until this task completes
     */
    public Task(TaskId taskId, TaskType taskType, List<Task> childrenTasks)
    {
        this(taskId, taskType, childrenTasks, Maps.newHashMap());
    }

    /**
     * @param taskId this task's ID - must be unique
     * @param taskType the task type
     * @param childrenTasks child tasks - children are not executed until this task completes
     * @param metaData meta data for the task
     */
    public Task(TaskId taskId, TaskType taskType, List<Task> childrenTasks, Map<String, String> metaData)
    {
        metaData = Preconditions.checkNotNull(metaData, "metaData cannot be null");
        childrenTasks = Preconditions.checkNotNull(childrenTasks, "childrenTasks cannot be null");
        this.taskId = Preconditions.checkNotNull(taskId, "taskId cannot be null");
        this.taskType = Optional.ofNullable(taskType);

        this.metaData = ImmutableMap.copyOf(metaData);
        this.childrenTasks = ImmutableList.copyOf(childrenTasks);
    }

    public List<Task> getChildrenTasks()
    {
        return childrenTasks;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskType getTaskType()
    {
        //noinspection ConstantConditions
        return taskType.get();       // exception if empty is desired
    }

    public boolean isExecutable()
    {
        return taskType.isPresent();
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

        Task task = (Task)o;

        if ( !childrenTasks.equals(task.childrenTasks) )
        {
            return false;
        }
        if ( !taskId.equals(task.taskId) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !taskType.equals(task.taskType) )
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
        result = 31 * result + childrenTasks.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Task{" +
            "taskId=" + taskId +
            ", taskType=" + taskType +
            ", childrenTasks=" + childrenTasks +
            '}';
    }
}
