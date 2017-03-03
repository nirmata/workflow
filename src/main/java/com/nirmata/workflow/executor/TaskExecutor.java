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
package com.nirmata.workflow.executor;

import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.models.ExecutableTask;

/**
 * Factory for creating task executions
 */
@FunctionalInterface
public interface TaskExecutor
{
    /**
     * Create a task execution for the given task
     *
     * @param workflowManager the manager
     * @param executableTask the task
     * @return the execution
     */
    TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask executableTask);
}
