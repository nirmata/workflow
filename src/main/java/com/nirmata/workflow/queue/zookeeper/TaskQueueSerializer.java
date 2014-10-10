package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTask;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import static com.nirmata.workflow.details.JsonSerializer.*;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTask>
{
    @Override
    public byte[] serialize(ExecutableTask executableTask)
    {
        return toBytes(newExecutableTask(executableTask));
    }

    @Override
    public ExecutableTask deserialize(byte[] bytes)
    {
        return getExecutableTask(fromBytes(bytes));
    }
}
