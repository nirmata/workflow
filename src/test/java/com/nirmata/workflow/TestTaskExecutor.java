package com.nirmata.workflow;

import com.google.common.collect.Maps;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskExecutionResult;
import java.util.concurrent.CountDownLatch;

class TestTaskExecutor implements TaskExecutor
{
    private final ConcurrentTaskChecker checker = new ConcurrentTaskChecker();
    private final int latchQty;
    private volatile CountDownLatch latch;

    public TestTaskExecutor()
    {
        this(1);
    }

    public TestTaskExecutor(int latchQty)
    {
        this.latchQty = latchQty;
        latch = new CountDownLatch(latchQty);
    }

    CountDownLatch getLatch()
    {
        return latch;
    }

    ConcurrentTaskChecker getChecker()
    {
        return checker;
    }

    void reset()
    {
        checker.reset();
        latch = new CountDownLatch(latchQty);
    }

    @Override
    public TaskExecution newTaskExecution(WorkflowManager workflowManager, ExecutableTask task)
    {
        return () -> {
            try
            {
                checker.add(task.getTaskId());
                doRun(task);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            finally
            {
                checker.decrement();
                latch.countDown();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "hey", Maps.<String, String>newHashMap());
        };
    }

    @SuppressWarnings("UnusedParameters")
    protected void doRun(ExecutableTask task) throws InterruptedException
    {
        Thread.sleep(1000);
    }
}
