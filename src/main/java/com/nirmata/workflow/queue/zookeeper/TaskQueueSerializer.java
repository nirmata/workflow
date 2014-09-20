package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.models.ExecutableTaskModel;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTaskModel>
{
    @Override
    public byte[] serialize(ExecutableTaskModel executableTask)
    {
        return toBytes(newExecutableTask(executableTask));
    }

    @Override
    public ExecutableTaskModel deserialize(byte[] bytes)
    {
        return getExecutableTask(fromBytes(bytes));
    }
}
