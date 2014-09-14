package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.spi.JsonSerializer;

public class InternalJsonSerializer
{
    public static ObjectNode addCompletedTask(ObjectNode node, CompletedTaskModel completedTask)
    {
        node.put("isComplete", completedTask.isComplete());
        node.putPOJO("resultData", completedTask.getResultData());
        return node;
    }

    public static CompletedTaskModel getCompletedTask(JsonNode node)
    {
        return new CompletedTaskModel
            (
                node.get("isComplete").asBoolean(),
                JsonSerializer.getMap(node.get("resultData"))
            );
    }

    public static ObjectNode addExecutableTask(ObjectNode node, ExecutableTaskModel executableTask)
    {
        node.put("scheduleId", executableTask.getScheduleId().getId());
        JsonSerializer.addTask(node, executableTask.getTask());
        return node;
    }

    public static ExecutableTaskModel getExecutableTask(JsonNode node)
    {
        return new ExecutableTaskModel
        (
            new ScheduleId(node.get("scheduleId").asText()),
            JsonSerializer.getTask(node)
        );
    }

    public static ObjectNode addDenormalizedWorkflow(ObjectNode node, DenormalizedWorkflowModel denormalizedWorkflow)
    {
        node.put("scheduleId", denormalizedWorkflow.getScheduleId().getId());
        node.put("workflowId", denormalizedWorkflow.getWorkflowId().getId());
        node.put("name", denormalizedWorkflow.getName());
        JsonSerializer.addTaskSet(node, denormalizedWorkflow.getTaskSets());
        JsonSerializer.addTasks(node, denormalizedWorkflow.getTasks());
        node.put("startDate", JsonSerializer.toString(denormalizedWorkflow.getStartDateUtc()));
        node.put("taskSetsIndex", denormalizedWorkflow.getTaskSetsIndex());
        return node;
    }

    public static DenormalizedWorkflowModel getDenormalizedWorkflow(JsonNode node)
    {
        return new DenormalizedWorkflowModel
        (
            new ScheduleId(node.get("scheduleId").asText()),
            new WorkflowId(node.get("workflowId").asText()),
            JsonSerializer.getTasks(node),
            node.get("name").asText(),
            JsonSerializer.getTaskSet(node),
            JsonSerializer.dateFromString(node.get("startDate").asText()),
            node.get("taskSetsIndex").asInt()
        );
    }

    private InternalJsonSerializer()
    {
    }
}
