package com.nirmata.workflow.details;

import com.nirmata.workflow.WorkflowManager;
import org.apache.curator.utils.ThreadUtils;
import java.io.Closeable;
import java.util.concurrent.ExecutorService;

public class TaskRunner implements Closeable
{
    private final WorkflowManager workflowManager;
    private final ExecutorService executorService;

    public TaskRunner(WorkflowManager workflowManager)
    {
        this.workflowManager = workflowManager;
        executorService = ThreadUtils.newFixedThreadPool(workflowManager.getConfiguration().getMaxTaskRunners(), "TaskRunner");
    }

    public void start()
    {

    }

    @Override
    public void close()
    {

    }
}
