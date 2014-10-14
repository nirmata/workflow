import com.google.common.collect.Lists;
import com.nirmata.workflow.events.WorkflowEvent;
import com.nirmata.workflow.events.WorkflowListener;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import java.io.Closeable;
import java.util.concurrent.CountDownLatch;

public class Example implements Closeable
{
    private final TestingServer testingServer;
    private final CuratorFramework curator;

    public static void main(String[] args) throws Exception
    {
        Example example = new Example();
        example.runExample();
        example.close();
    }

    public Example() throws Exception
    {
        // for testing purposes, start an in-memory test ZooKeeper instance
        testingServer = new TestingServer();

        // allocate the Curator instance
        curator = CuratorFrameworkFactory.builder()
            .connectString(testingServer.getConnectString())
            .retryPolicy(new ExponentialBackoffRetry(100, 3))
            .build();
    }

    public void runExample()
    {
        curator.start();

        // for our example, we'll just have one task type
        TaskType taskType = new TaskType("my type", "1", true);

        // a task which will have two parents
        Task childTask = new Task(new TaskId("child task"), taskType);
        Task parentTask1 = new Task(new TaskId("parent 1"), taskType, Lists.newArrayList(childTask));
        Task parentTask2 = new Task(new TaskId("parent 2"), taskType, Lists.newArrayList(childTask));
        Task rootTask = new Task(new TaskId(), Lists.newArrayList(parentTask1, parentTask2));

        // an executor that just logs a message and returns
        ExampleTaskExecutor taskExecutor = new ExampleTaskExecutor();

        // allocate the workflow manager with some executors for our type
        WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build();

        WorkflowListenerManager workflowListenerManager = workflowManager.newWorkflowListenerManager();
        try
        {
            // listen for run completion and count down a latch when it happens
            final CountDownLatch doneLatch = new CountDownLatch(1);
            WorkflowListener listener = new WorkflowListener()
            {
                @Override
                public void receiveEvent(WorkflowEvent event)
                {
                    if ( event.getType() == WorkflowEvent.EventType.RUN_UPDATED )
                    {
                        doneLatch.countDown();  // note: the run could have had an error. RUN_UPDATED does not guarantee successful completion
                    }
                }
            };
            workflowListenerManager.getListenable().addListener(listener);

            // start the manager and the listeners
            workflowManager.start();
            workflowListenerManager.start();

            // submit our task
            workflowManager.submitTask(rootTask);

            // you should see these messages in the console:
            //      Executing task: Id{id='parent 1'}
            //      Executing task: Id{id='parent 2'}
            // then
            //      Executing task: Id{id='child task'}


            // wait for completion
            doneLatch.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            CloseableUtils.closeQuietly(workflowListenerManager);
            CloseableUtils.closeQuietly(workflowManager);
        }
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(testingServer);
    }
}
