package com.nirmata.workflow.queue.zookeeper;

import com.nirmata.workflow.details.ExecutableTaskModel;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import static com.nirmata.workflow.details.JsonSerializer.*;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTaskModel>
{
    @Override
    public byte[] serialize(ExecutableTaskModel executableTask)
    {
        return null;// TODO toBytes(newExecutableTask(executableTask));
    }

    @Override
    public ExecutableTaskModel deserialize(byte[] bytes)
    {
        return null; // TODO getExecutableTask(fromBytes(bytes));
    }
}
