package com.nirmata.workflow;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import java.io.Closeable;
import java.util.Map;

public interface WorkflowManager extends Closeable
{
    public void start();

    public RunId submitTask(Task task);

    public RunId submitSubTask(Task task, RunId parentRunId, TaskId parentTaskId);

    public Map<String, String> getTaskData(RunId runId, TaskId taskId);
}
