package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.Queue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperQueue implements Queue
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DistributedQueue<ExecutableTask> queue;

    public ZooKeeperQueue(CuratorFramework curator, TaskType taskType)
    {
        curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        String path = ZooKeeperConstants.getQueuePath(taskType);
        QueueBuilder<ExecutableTask> builder = QueueBuilder.builder(curator, null, new TaskQueueSerializer(), path);
        if ( taskType.isIdempotent() )
        {
            builder = builder.lockPath(ZooKeeperConstants.getQueuePath(taskType));
        }
        queue = builder.buildQueue();
    }

    @Override
    public void put(ExecutableTask executableTask)
    {
        try
        {
            queue.put(executableTask);
        }
        catch ( Exception e )
        {
            log.error("Could not add to queue for: " + executableTask, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start()
    {
        try
        {
            queue.start();
        }
        catch ( Exception e )
        {
            log.error("Could not start queue", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(queue);
    }
}
