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

package com.nirmata.workflow.storage;

import java.util.List;
import java.util.Map;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;

/**
 * Storage manager to store and retrieve binary data corresponding to
 * various objects pertaining to Runs, Tasks, Results, etc.
 */
public interface StorageManager {

    /**
     * Get all runIds in DB
     * 
     * @return all RunIds
     */
    List<String> getRunIds();

    /**
     * Get all runs from DB
     * 
     * @return all RunRecords
     */
    Map<String, byte[]> getRuns();

    /**
     * Get a specific runId's runnable's serialized data
     * 
     * @param runId
     * @return bytes to be deserialied to RunnableTask
     */
    byte[] getRunnable(RunId runId);

    /**
     * Get all serialized details for a run
     * 
     * @param runId
     * @return a RunRecord
     */
    RunRecord getRunDetails(RunId runId);

    /**
     * Create a new runnable
     * 
     * @param runId
     * @param runnableTaskBytes serialized from a RunnableTask
     */
    void createRun(RunId runId, byte[] runnableTaskBytes);

    /**
     * Update a runnable
     * 
     * @param runId
     * @param runnableTaskBytes serialized from a RunnableTask
     */
    void updateRun(RunId runId, byte[] runnableTaskBytes);

    /**
     * Get serialized data corresponding to TaskExecutionResult
     * 
     * @param runId
     * @param taskId
     * @return
     */
    byte[] getTaskExecutionResult(RunId runId, TaskId taskId);

    /**
     * Save results of a task execution
     * 
     * @param runId
     * @param taskId
     * @param taskResultData
     */
    void saveTaskResult(RunId runId, TaskId taskId, byte[] taskResultData);

    /**
     * Get serialized data corresponding to a StartedTask object
     * 
     * @param runId
     * @param taskId
     * @return
     */
    byte[] getStartedTask(RunId runId, TaskId taskId);

    /**
     * Set started task's data
     * 
     * @param runId
     * @param taskId
     * @param startedTaskData
     */
    void setStartedTask(RunId runId, TaskId taskId, byte[] startedTaskData);

    /**
     * Delete a run record
     * 
     * @param runId
     * @return
     */
    boolean clean(RunId runId);
}
