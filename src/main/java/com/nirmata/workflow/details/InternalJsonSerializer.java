package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.spi.Clock;

import static com.nirmata.workflow.spi.JsonSerializer.*;

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
                getMap(node.get("resultData"))
            );
    }

    public static ObjectNode addExecutableTask(ObjectNode node, ExecutableTaskModel executableTask)
    {
        node.put("scheduleId", executableTask.getScheduleId().getId());
        addTask(node, executableTask.getTask());
        return node;
    }

    public static ExecutableTaskModel getExecutableTask(JsonNode node)
    {
        return new ExecutableTaskModel
        (
            new ScheduleId(node.get("scheduleId").asText()),
            getTask(node)
        );
    }

    public static ObjectNode addDenormalizedWorkflow(ObjectNode node, DenormalizedWorkflowModel denormalizedWorkflow)
    {
        addScheduleExecution(node, denormalizedWorkflow.getScheduleExecution());
        node.put("workflowId", denormalizedWorkflow.getWorkflowId().getId());
        node.put("name", denormalizedWorkflow.getName());
        addTaskSet(node, denormalizedWorkflow.getTaskSets());
        addTasks(node, denormalizedWorkflow.getTasks());
        node.put("startDate", Clock.dateToString(denormalizedWorkflow.getStartDateUtc()));
        node.put("taskSetsIndex", denormalizedWorkflow.getTaskSetsIndex());
        return node;
    }

    public static DenormalizedWorkflowModel getDenormalizedWorkflow(JsonNode node)
    {
        return new DenormalizedWorkflowModel
        (
            getScheduleExecution(node),
            new WorkflowId(node.get("workflowId").asText()),
            getTasks(node),
            node.get("name").asText(),
            getTaskSet(node),
            Clock.dateFromString(node.get("startDate").asText()),
            node.get("taskSetsIndex").asInt()
        );
    }

    private InternalJsonSerializer()
    {
    }
}
