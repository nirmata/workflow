package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.StartedTaskModel;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.ExecutableTaskModel;
import com.nirmata.workflow.models.ScheduleId;
import com.nirmata.workflow.spi.TaskExecutionResult;
import com.nirmata.workflow.models.WorkflowId;
import com.nirmata.workflow.spi.Clock;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class InternalJsonSerializer
{
    public static ObjectNode addStartedTask(ObjectNode node, StartedTaskModel startedTask)
    {
        node.put("startDateUtc", Clock.dateToString(startedTask.getStartDateUtc()));
        return node;
    }

    public static StartedTaskModel getStartedTask(JsonNode node)
    {
        return new StartedTaskModel(Clock.dateFromString(node.get("startDateUtc").asText()));
    }

    public static ObjectNode addTaskExecutionResult(ObjectNode node, TaskExecutionResult taskExecutionResult)
    {
        node.put("details", taskExecutionResult.getDetails());
        node.putPOJO("resultData", taskExecutionResult.getResultData());
        node.put("completionDateUtc", Clock.dateToString(taskExecutionResult.getCompletionDateUtc()));
        return node;
    }

    public static TaskExecutionResult getTaskExecutionResult(JsonNode node)
    {
        return new TaskExecutionResult
        (
            node.get("details").asText(),
            getMap(node.get("resultData")),
            Clock.dateFromString(node.get("completionDateUtc").asText())
        );
    }

    public static ObjectNode addExecutableTask(ObjectNode node, ExecutableTaskModel executableTask)
    {
        node.put("runId", executableTask.getRunId().getId());
        node.put("scheduleId", executableTask.getScheduleId().getId());
        addTask(node, executableTask.getTask());
        return node;
    }

    public static ExecutableTaskModel getExecutableTask(JsonNode node)
    {
        return new ExecutableTaskModel
        (
            new RunId(node.get("runId").asText()),
            new ScheduleId(node.get("scheduleId").asText()),
            getTask(node)
        );
    }

    public static ObjectNode addDenormalizedWorkflow(ObjectNode node, DenormalizedWorkflowModel denormalizedWorkflow)
    {
        addId(node, denormalizedWorkflow.getRunId());
        addScheduleExecution(node, denormalizedWorkflow.getScheduleExecution());
        node.put("workflowId", denormalizedWorkflow.getWorkflowId().getId());
        node.put("name", denormalizedWorkflow.getName());
        addTaskSet(node, denormalizedWorkflow.getTaskSets());
        addTasks(node, denormalizedWorkflow.getTasks());
        node.put("startDateUtc", Clock.dateToString(denormalizedWorkflow.getStartDateUtc()));
        node.put("taskSetsIndex", denormalizedWorkflow.getTaskSetsIndex());
        return node;
    }

    public static DenormalizedWorkflowModel getDenormalizedWorkflow(JsonNode node)
    {
        return new DenormalizedWorkflowModel
        (
            new RunId(getId(node)),
            getScheduleExecution(node),
            new WorkflowId(node.get("workflowId").asText()),
            getTasks(node),
            node.get("name").asText(),
            getTaskSet(node),
            Clock.dateFromString(node.get("startDateUtc").asText()),
            node.get("taskSetsIndex").asInt()
        );
    }

    private InternalJsonSerializer()
    {
    }
}
