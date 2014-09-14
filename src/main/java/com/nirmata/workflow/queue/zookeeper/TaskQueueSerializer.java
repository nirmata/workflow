package com.nirmata.workflow.queue.zookeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.InternalJsonSerializer;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.spi.JsonSerializer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTaskModel>
{
    @Override
    public byte[] serialize(ExecutableTaskModel executableTask)
    {
        ObjectNode node = JsonSerializer.newNode();
        InternalJsonSerializer.addExecutableTask(node, executableTask);
        return JsonSerializer.toString(node).getBytes();
    }

    @Override
    public ExecutableTaskModel deserialize(byte[] bytes)
    {
        JsonNode node = JsonSerializer.fromString(new String(bytes));
        return InternalJsonSerializer.getExecutableTask(node);
    }
}
