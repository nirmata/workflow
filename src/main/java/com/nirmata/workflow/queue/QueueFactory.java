package com.nirmata.workflow.queue;

public interface QueueFactory
{
    public Queue createIdempotentQueue();
    public Queue createNonIdempotentQueue();
    public QueueConsumer createIdempotentQueueConsumer();
    public QueueConsumer createNonIdempotentQueueConsumer();
}
