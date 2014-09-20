package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.WorkflowId;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class InternalJsonSerializer
{
    public static ObjectNode newDenormalizedWorkflow(DenormalizedWorkflowModel denormalizedWorkflow)
    {
        ObjectNode node = newNode();
        addId(node, denormalizedWorkflow.getRunId());
        node.set("scheduleExecution", newScheduleExecution(denormalizedWorkflow.getScheduleExecution()));
        node.put("workflowId", denormalizedWorkflow.getWorkflowId().getId());
        node.put("name", denormalizedWorkflow.getName());
        node.set("tasksSet", newTaskSet(denormalizedWorkflow.getTaskSets()));
        node.set("tasks", newTasks(denormalizedWorkflow.getTasks()));
        node.put("startDateUtc", denormalizedWorkflow.getStartDateUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("taskSetsIndex", denormalizedWorkflow.getTaskSetsIndex());
        return node;
    }

    public static DenormalizedWorkflowModel getDenormalizedWorkflow(JsonNode node)
    {
        return new DenormalizedWorkflowModel
        (
            new RunId(getId(node)),
            getScheduleExecution(node.get("scheduleExecution")),
            new WorkflowId(node.get("workflowId").asText()),
            getTasks(node.get("tasks")),
            node.get("name").asText(),
            getTaskSet(node.get("tasksSet")),
            LocalDateTime.parse(node.get("startDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME),
            node.get("taskSetsIndex").asInt()
        );
    }

    private InternalJsonSerializer()
    {
    }
}
