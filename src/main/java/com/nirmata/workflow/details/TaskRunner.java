package com.nirmata.workflow.details;

import com.nirmata.workflow.WorkflowManager;

public class TaskRunner implements Runnable
{
    private final WorkflowManager workflowManager;

    public TaskRunner(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
    }

    @Override
    public void run()
    {

    }
}
