package com.nirmata.workflow;

import com.google.common.collect.Queues;
import com.nirmata.workflow.admin.WorkflowEvent;
import com.nirmata.workflow.admin.WorkflowListenerManager;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestWorkflowListenerManager extends BaseForTests
{
    @Test
    public void testBasic() throws Exception
    {
        WorkflowListenerManager workflowListenerManager = null;
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        TaskType taskType = new TaskType("test", "1", true);
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();
        try
        {
            Task task = new Task(new TaskId(), taskType);

            BlockingQueue<WorkflowEvent> eventQueue = Queues.newLinkedBlockingQueue();
            workflowListenerManager = workflowManager.getAdmin().newWorkflowListenerManager();
            workflowListenerManager.getListenable().addListener(eventQueue::add);

            workflowManager.start();
            workflowListenerManager.start();

            RunId runId = workflowManager.submitTask(task);

            Timing timing = new Timing();
            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.RUN_STARTED, runId));
            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.TASK_STARTED, runId, task.getTaskId()));
            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.TASK_COMPLETED, runId, task.getTaskId()));
            Assert.assertEquals(eventQueue.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), new WorkflowEvent(WorkflowEvent.EventType.RUN_UPDATED, runId));
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowListenerManager);
            CloseableUtils.closeQuietly(workflowManager);
        }
    }
}
