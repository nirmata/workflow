package com.nirmata.workflow.queue.zookeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.InternalJsonSerializer;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import static com.nirmata.workflow.details.InternalJsonSerializer.*;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTaskModel>
{
    @Override
    public byte[] serialize(ExecutableTaskModel executableTask)
    {
        ObjectNode node = newNode();
        addExecutableTask(node, executableTask);
        return toBytes(node);
    }

    @Override
    public ExecutableTaskModel deserialize(byte[] bytes)
    {
        JsonNode node = fromBytes(bytes);
        return getExecutableTask(node);
    }
}
