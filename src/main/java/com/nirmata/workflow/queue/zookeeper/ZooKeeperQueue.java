package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.ExecutableTaskModel;
import com.nirmata.workflow.details.ZooKeeperConstants;
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
    private final DistributedQueue<ExecutableTaskModel> queue;

    public ZooKeeperQueue(CuratorFramework curator, boolean idempotent)
    {
        curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        String path = idempotent ? ZooKeeperConstants.getIdempotentTasksQueuePath() : ZooKeeperConstants.getNonIdempotentTasksQueuePath();
        QueueBuilder<ExecutableTaskModel> builder = QueueBuilder.builder(curator, null, new TaskQueueSerializer(), path);
        if ( idempotent )
        {
            builder = builder.lockPath(ZooKeeperConstants.getIdempotentTasksQueueLockPath());
        }
        queue = builder.buildQueue();
    }

    @Override
    public void put(ExecutableTaskModel executableTask)
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
