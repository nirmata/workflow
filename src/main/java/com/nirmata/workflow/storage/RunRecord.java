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

import java.util.Map;

/**
 * This contains all the database information corresponding to a run. The
 * runnable with task DAG, task progress and execution results. This is
 * communicated to external callers who do necessary deserialization
 */
public class RunRecord {
    private final byte[] runnableData;
    private final Map<String, byte[]> startedTasks;
    private final Map<String, byte[]> completedTasks;

    public RunRecord(byte[] runnableTaskData, Map<String, byte[]> startedTasks, Map<String, byte[]> completedTasks) {
        this.runnableData = runnableTaskData;
        this.startedTasks = startedTasks;
        this.completedTasks = completedTasks;
    }

    public byte[] getRunnableData() {
        return runnableData;
    }

    public Map<String, byte[]> getStartedTasks() {
        return startedTasks;
    }

    public Map<String, byte[]> getCompletedTasks() {
        return completedTasks;
    }

}
