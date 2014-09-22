package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagEntryModel;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDagModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.WorkflowId;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class InternalJsonSerializer
{
    public static JsonNode newDenormalizedWorkflow(DenormalizedWorkflowModel denormalizedWorkflow)
    {
        ObjectNode node = newNode();
        addId(node, denormalizedWorkflow.getRunId());
        node.set("scheduleExecution", newScheduleExecution(denormalizedWorkflow.getScheduleExecution()));
        node.put("workflowId", denormalizedWorkflow.getWorkflowId().getId());
        node.put("name", denormalizedWorkflow.getName());
        node.set("taskDag", newTaskDag(denormalizedWorkflow.getTaskDag()));
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
            getTaskDag(node.get("taskDag")),
            LocalDateTime.parse(node.get("startDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME),
            node.get("taskSetsIndex").asInt()
        );
    }

    public static JsonNode newRunnableTaskDagEntry(RunnableTaskDagEntryModel runnableTaskDagEntry)
    {
        ArrayNode tab = newArrayNode();
        for ( TaskId taskId : runnableTaskDagEntry.getDependencies() )
        {
            tab.add(taskId.getId());
        }

        ObjectNode node = newNode();
        node.put("taskId", runnableTaskDagEntry.getTaskId().getId());
        node.set("dependencies", tab);
        return node;
    }

    public static RunnableTaskDagEntryModel getRunnableTaskDagEntry(JsonNode node)
    {
        List<TaskId> dependencies = Lists.newArrayList();
        for ( JsonNode child : node.get("dependencies") )
        {
            dependencies.add(new TaskId(child.asText()));
        }
        TaskId taskId = new TaskId(node.get("taskId").asText());
        return new RunnableTaskDagEntryModel(taskId, dependencies);
    }

    public static JsonNode newRunnableTaskDagModel(RunnableTaskDagModel runnableTaskDag)
    {
        ArrayNode tab = newArrayNode();
        for ( RunnableTaskDagEntryModel entry : runnableTaskDag.getEntries() )
        {
            tab.add(newRunnableTaskDagEntry(entry));
        }
        return tab;
    }

    public static RunnableTaskDagModel getRunnableTaskDagModel(JsonNode node)
    {
        List<RunnableTaskDagEntryModel> entries = Lists.newArrayList();
        for ( JsonNode child : node )
        {
            entries.add(getRunnableTaskDagEntry(child));
        }
        return new RunnableTaskDagModel(entries);
    }

    private InternalJsonSerializer()
    {
    }
}
