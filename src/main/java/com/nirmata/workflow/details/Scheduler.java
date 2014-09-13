package com.nirmata.workflow.details;

import com.nirmata.workflow.WorkflowManager;

public class Scheduler implements Runnable
{
    private final WorkflowManager workflowManager;

    public Scheduler(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
    }

    @Override
    public void run()
    {

    }
}
