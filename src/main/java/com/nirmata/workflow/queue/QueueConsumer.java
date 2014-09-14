package com.nirmata.workflow.queue;

import java.io.Closeable;

public interface QueueConsumer extends Closeable
{
    public void start();

    @Override
    public void close();
}
