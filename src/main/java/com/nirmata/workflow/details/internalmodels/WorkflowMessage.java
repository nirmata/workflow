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

import java.io.Serializable;
import java.util.Optional;

import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;

/**
 * A workflow message to the workflow scheduler. All types of messages are
 * multiplexed in the same message, and the type field determines which
 * sub-message it is. Either a new workflow submission, or a task result,
 * cancel, or retry of workflow.
 */
public class WorkflowMessage implements Serializable {

    public enum MsgType {
        TASK, TASKRESULT, CANCEL
    };

    private final MsgType msgType;
    private boolean isRetry = false;

    private Optional<RunnableTask> runnableTask = Optional.ofNullable(null);
    private Optional<TaskId> taskId = Optional.ofNullable(null);
    private Optional<TaskExecutionResult> taskExecResult = Optional.ofNullable(null);

    public WorkflowMessage() {
        this.msgType = MsgType.CANCEL;
    }

    public WorkflowMessage(RunnableTask rt) {
        this(rt, false);
    }

    public WorkflowMessage(RunnableTask rt, boolean isRetry) {
        this.msgType = MsgType.TASK;
        this.isRetry = isRetry;
        this.runnableTask = Optional.ofNullable(rt);
    }

    public WorkflowMessage(TaskId taskId, TaskExecutionResult res) {
        this.msgType = MsgType.TASKRESULT;
        this.taskId = Optional.ofNullable(taskId);
        this.taskExecResult = Optional.ofNullable(res);
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public Optional<RunnableTask> getRunnableTask() {
        return runnableTask;
    }

    public Optional<TaskId> getTaskId() {
        return taskId;
    }

    public Optional<TaskExecutionResult> getTaskExecResult() {
        return taskExecResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WorkflowMessage that = (WorkflowMessage) o;

        if (!msgType.equals(that.msgType)) {
            return false;
        }
        if (!isRetry == that.isRetry) {
            return false;
        }
        if (!runnableTask.equals(that.runnableTask)) {
            return false;
        }
        if (!taskId.equals(that.taskId)) {
            return false;
        }
        if (!taskExecResult.equals(that.taskExecResult)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "WorkflowMessage{" +
                "msgType=" + msgType +
                ", isRetry=" + isRetry +
                ", runnableTask=" + runnableTask +
                ", taskId=" + taskId +
                ", taskExecResult=" + taskExecResult +
                '}';
    }

}
