package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTask;
import org.apache.curator.framework.recipes.queue.DistributedDelayQueue;
import java.io.IOException;

class DelayQueue implements InternalQueueBase
{
    private final DistributedDelayQueue<ExecutableTask> queue;

    DelayQueue(DistributedDelayQueue<ExecutableTask> queue)
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
        queue.put(item, Math.max(1, value));
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }
}
