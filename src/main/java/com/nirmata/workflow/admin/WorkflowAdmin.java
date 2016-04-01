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

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import java.util.List;
import java.util.Map;

/**
 * Admin operations
 */
public interface WorkflowAdmin
{
    /**
     * Return all run IDs completed or currently executing
     * in the workflow manager
     *
     * @return run infos
     */
    List<RunId> getRunIds();

    /**
     * Return info about all runs completed or currently executing
     * in the workflow manager
     *
     * @return run infos
     */
    List<RunInfo> getRunInfo();

    /**
     * Return info about the given run
     *
     * @param runId run
     * @return info
     */
    RunInfo getRunInfo(RunId runId);

    /**
     * Return info about all the tasks completed, started or waiting for
     * the given run
     *
     * @param runId run
     * @return task infos
     */
    List<TaskInfo> getTaskInfo(RunId runId);

    /**
     * Returns a map of all task details for the given run
     *
     * @param runId run
     * @return task details
     */
    Map<TaskId, TaskDetails> getTaskDetails(RunId runId);

    /**
     * Delete all saved data for the given run.
     *
     * @param runId the run
     * @return true if the run was found
     */
    boolean clean(RunId runId);

    /**
     * Return information about the internal run/state of the workflow manager
     *
     * @return state
     */
    WorkflowManagerState getWorkflowManagerState();
}
