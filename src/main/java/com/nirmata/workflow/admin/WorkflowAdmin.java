package com.nirmata.workflow.admin;

import com.nirmata.workflow.models.RunId;
import java.util.List;

public interface WorkflowAdmin
{
    public List<RunInfo> getRunInfo();

    public List<TaskInfo> getTaskInfo(RunId runId);
}
