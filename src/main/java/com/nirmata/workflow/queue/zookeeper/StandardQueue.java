package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTask;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import java.io.IOException;

class StandardQueue implements InternalQueueBase
{
    private final DistributedQueue<ExecutableTask> queue;

    StandardQueue(DistributedQueue<ExecutableTask> queue)
    {
        this.queue = queue;
    }

    @Override
    public void start() throws Exception
    {
        queue.start();
    }

    @Override
    public void put(ExecutableTask item, long value) throws Exception
    {
        queue.put(item);
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }
}
