package com.nirmata.workflow;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Queue;
import java.util.Set;

public class TestEdges extends BaseForTests
{
    @Test
    public void testIdempotency() throws Exception
    {
        TaskType idempotentType = new TaskType("yes", "1", true);
        TaskType nonIdempotentType = new TaskType("no", "1", false);

        Task idempotentTask = new Task(new TaskId(), idempotentType);
        Task nonIdempotentTask = new Task(new TaskId(), nonIdempotentType);
        Task root = new Task(new TaskId(), Lists.newArrayList(idempotentTask, nonIdempotentTask));

        Set<TaskId> thrownTasks = Sets.newConcurrentHashSet();
        Queue<TaskId> tasks = Queues.newConcurrentLinkedQueue();
        TaskExecutor taskExecutor = (m, t) -> () -> {
            if ( thrownTasks.add(t.getTaskId()) )
            {
                throw new RuntimeException();
            }
            tasks.add(t.getTaskId());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, idempotentType)
            .addingTaskExecutor(taskExecutor, 10, nonIdempotentType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            workflowManager.start();
            workflowManager.submitTask(root);

            new Timing().sleepABit();

            Assert.assertEquals(tasks.size(), 1);
            Assert.assertEquals(tasks.poll(), idempotentTask.getTaskId());
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
