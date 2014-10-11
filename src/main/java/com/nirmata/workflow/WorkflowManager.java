package com.nirmata.workflow;

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

public interface WorkflowManager extends Closeable
{
    public void start();

    public RunId submitTask(Task task);

    public Optional<Map<String, String>> getTaskData(RunId runId, TaskId taskId);

    public boolean cancelRun(RunId runId);
}
