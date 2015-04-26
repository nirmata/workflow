package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.ExecutableTask;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import java.io.IOException;

class PriorityQueue implements InternalQueueBase
{
    private final DistributedPriorityQueue<ExecutableTask> queue;

    PriorityQueue(DistributedPriorityQueue<ExecutableTask> queue)
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
        Preconditions.checkArgument(value <= Integer.MAX_VALUE, "priority is too large: " + value);
        queue.put(item, (int)value);
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }
}
