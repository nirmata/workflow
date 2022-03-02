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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;

// In case workflow does not need functionality such as avoiding duplicate workflows with same name, 
// or retrying failed workflows, needing latest status of workflows, or storing run history
// the no-op implementation can be used
public class StorageManagerNoOpImpl implements StorageManager {

    @Override
    public byte[] getRunnable(RunId runId) {
        return null;
    }

    @Override
    public byte[] getTaskExecutionResult(RunId runId, TaskId taskId) {
        return null;
    }

    @Override
    public boolean clean(RunId runId) {
        return true;
    }

    @Override
    public List<String> getRunIds() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, byte[]> getRuns() {
        return Collections.emptyMap();
    }

    @Override
    public RunRecord getRunDetails(RunId runId) {
        // No Op
        return null;
    }

    @Override
    public void createRun(RunId runId, byte[] runnableTaskBytes) {
        // No Op

    }

    @Override
    public void updateRun(RunId runId, byte[] runnableTaskBytes) {
        // No Op

    }

    @Override
    public void saveTaskResult(RunId runId, TaskId taskId, byte[] taskResultData) {
        // No Op

    }

    @Override
    public void setStartedTask(RunId runId, TaskId taskId, byte[] startedTaskData) {
        // No Op

    }

    @Override
    public byte[] getStartedTask(RunId runId, TaskId taskId) {
        // No Op
        return null;
    }

}
