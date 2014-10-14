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
package com.nirmata.workflow.details;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.TaskType;

public class TaskExecutorSpec
{
    private final TaskExecutor taskExecutor;
    private final int qty;
    private final TaskType taskType;

    public TaskExecutorSpec(TaskExecutor taskExecutor, int qty, TaskType taskType)
    {
        this.taskType = Preconditions.checkNotNull(taskType, "taskType cannot be null");
        this.taskExecutor = Preconditions.checkNotNull(taskExecutor, "taskExecutor cannot be null");
        this.qty = qty;
    }

    TaskExecutor getTaskExecutor()
    {
        return taskExecutor;
    }

    int getQty()
    {
        return qty;
    }

    TaskType getTaskType()
    {
        return taskType;
    }
}
