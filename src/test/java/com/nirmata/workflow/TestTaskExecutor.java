package com.nirmata.workflow;

import com.google.common.collect.Maps;
import com.nirmata.workflow.models.ExecutableTaskModel;
import com.nirmata.workflow.spi.TaskExecution;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.spi.TaskExecutor;
import java.util.concurrent.CountDownLatch;

class TestTaskExecutor implements TaskExecutor
{
    private final ConcurrentTaskChecker checker = new ConcurrentTaskChecker();
    private final int latchQty;
    private volatile CountDownLatch latch;

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
    public TaskExecution newTaskExecution(final ExecutableTaskModel task)
    {
        return () -> {
            try
            {
                checker.add(task.getTask().getTaskId());
                Thread.sleep(1000);
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
            return new TaskExecutionResult("hey", Maps.<String, String>newHashMap());
        };
    }
}
