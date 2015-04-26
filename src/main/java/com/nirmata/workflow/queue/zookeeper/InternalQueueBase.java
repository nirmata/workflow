package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTask;
import java.io.Closeable;

public interface InternalQueueBase extends Closeable
{
    void start() throws Exception;

    void put(ExecutableTask item, long value) throws Exception;
}
