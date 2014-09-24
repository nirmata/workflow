package com.nirmata.workflow.spi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.models.*;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Serializer/deserializer methods for the various models
 */
public class JsonSerializer
{
    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode newNode()
    {
        return mapper.createObjectNode();
    }

    public static ArrayNode newArrayNode()
    {
        return mapper.createArrayNode();
    }

    public static ObjectMapper getMapper()
    {
        return mapper;
    }

    public static byte[] toBytes(JsonNode node)
    {
        return nodeToString(node).getBytes();
    }

    public static String nodeToString(JsonNode node)
    {
        try
        {
            return mapper.writeValueAsString(node);
        }
        catch ( JsonProcessingException e )
        {
            log.error("mapper.writeValueAsString", e);
            throw new RuntimeException(e);
        }
    }

    public static JsonNode fromBytes(byte[] bytes)
    {
        return fromString(new String(bytes));
    }

    public static JsonNode fromString(String str)
    {
        try
        {
            return mapper.readTree(str);
        }
        catch ( IOException e )
        {
            log.error("reading JSON: " + str, e);
            throw new RuntimeException(e);
        }
    }

    public static JsonNode newTaskDag(TaskDagModel taskDag)
    {
        ArrayNode childrenNode = newArrayNode();
        taskDag.getChildren().forEach(d -> childrenNode.add(newTaskDag(d)));

        ObjectNode node = newNode();
        node.put("taskId", taskDag.getTaskId().getId());
        node.set("children", childrenNode);
        return node;
    }

    public static TaskDagModel getTaskDag(JsonNode node)
    {
        List<TaskDagModel> children = Lists.newArrayList();
        JsonNode childrenNode = node.get("children");
        if ( (childrenNode != null) && (childrenNode.size() > 0) )
        {
            childrenNode.forEach(n -> children.add(getTaskDag(n)));
        }

        return new TaskDagModel(new TaskId(node.get("taskId").asText()), children);
    }

    public static JsonNode newScheduleExecution(ScheduleExecutionModel scheduleExecution)
    {
        ObjectNode node = newNode();
        node.put("scheduleId", scheduleExecution.getScheduleId().getId());
        node.put("lastExecutionStartUtc", scheduleExecution.getLastExecutionStartUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("lastExecutionEndUtc", scheduleExecution.getLastExecutionEndUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("executionQty", scheduleExecution.getExecutionQty());
        return node;
    }

    public static ScheduleExecutionModel getScheduleExecution(JsonNode node)
    {
        return new ScheduleExecutionModel
        (
            new ScheduleId(node.get("scheduleId").asText()),
            LocalDateTime.parse(node.get("lastExecutionStartUtc").asText(), DateTimeFormatter.ISO_DATE_TIME),
            LocalDateTime.parse(node.get("lastExecutionEndUtc").asText(), DateTimeFormatter.ISO_DATE_TIME),
            node.get("executionQty").asInt()
        );
    }

    public static JsonNode newWorkflows(List<WorkflowModel> workflows)
    {
        ArrayNode tab = newArrayNode();
        for ( WorkflowModel workflow : workflows )
        {
            tab.add(newWorkflow(workflow));
        }
        return tab;
    }

    public static List<WorkflowModel> getWorkflows(JsonNode node)
    {
        ImmutableList.Builder<WorkflowModel> builder = ImmutableList.builder();
        Iterator<JsonNode> elements = node.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            builder.add(getWorkflow(next));
        }
        return builder.build();
    }

    public static JsonNode newWorkflow(WorkflowModel workflow)
    {
        ObjectNode node = newNode();
        addId(node, workflow.getWorkflowId());
        node.put("name", workflow.getName());
        node.put("taskDagId", workflow.getTaskDagId().getId());
        return node;
    }

    public static WorkflowModel getWorkflow(JsonNode node)
    {
        return new WorkflowModel
        (
            new WorkflowId(getId(node)),
            node.get("name").asText(),
            new TaskDagId(node.get("taskDagId").asText())
        );
    }

    public static JsonNode newSchedules(List<ScheduleModel> schedules)
    {
        ArrayNode tab = newArrayNode();
        for ( ScheduleModel schedule : schedules )
        {
            tab.add(newSchedule(schedule));
        }
        return tab;
    }

    public static List<ScheduleModel> getSchedules(JsonNode node)
    {
        ImmutableList.Builder<ScheduleModel> builder = ImmutableList.builder();
        Iterator<JsonNode> elements = node.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            builder.add(getSchedule(next));
        }
        return builder.build();
    }

    public static JsonNode newSchedule(ScheduleModel schedule)
    {
        ObjectNode node = newNode();
        addId(node, schedule.getScheduleId());
        node.set("repetition", newRepetition(schedule.getRepetition()));
        node.put("workflowId", schedule.getWorkflowId().getId());
        return node;
    }

    public static ScheduleModel getSchedule(JsonNode node)
    {
        return new ScheduleModel
        (
            new ScheduleId(getId(node)),
            new WorkflowId(node.get("workflowId").asText()),
            getRepetition(node.get("repetition"))
        );
    }

    public static JsonNode newTasks(Collection<TaskModel> tasks)
    {
        ArrayNode tab = newArrayNode();
        for ( TaskModel task : tasks )
        {
            tab.add(newTask(task));
        }
        return tab;
    }

    public static List<TaskModel> getTasks(JsonNode node)
    {
        ImmutableList.Builder<TaskModel> builder = ImmutableList.builder();
        Iterator<JsonNode> elements = node.elements();
        while ( elements.hasNext() )
        {
            JsonNode n = elements.next();
            builder.add(getTask(n));
        }
        return builder.build();
    }

    public static JsonNode newTask(TaskModel task)
    {
        ObjectNode node = newNode();
        addId(node, task.getTaskId());
        node.put("name", task.getName());
        node.put("code", task.getTaskExecutionCode());
        node.put("isIdempotent", task.isIdempotent());
        node.putPOJO("meta", task.getMetaData());
        return node;
    }

    public static TaskModel getTask(JsonNode node)
    {
        return new TaskModel
        (
            new TaskId(getId(node)),
            node.get("name").asText(),
            node.get("code").asText(),
            node.get("isIdempotent").asBoolean(),
            getMap(node.get("meta"))
        );
    }

    public static JsonNode newRepetition(RepetitionModel repetition)
    {
        ObjectNode node = newNode();
        node.put("duration", repetition.getDuration().toString());
        node.put("type", repetition.getType().name().toLowerCase());
        node.put("qty", repetition.getQty());
        return node;
    }

    public static RepetitionModel getRepetition(JsonNode node)
    {
        return new RepetitionModel
        (
            Duration.valueOf(node.get("duration").asText()),
            RepetitionModel.Type.valueOf(node.get("type").asText().toUpperCase()),
            node.get("qty").asInt()
        );
    }

    public static JsonNode newTaskExecutionResult(TaskExecutionResult taskExecutionResult)
    {
        ObjectNode node = newNode();
        node.put("status", taskExecutionResult.getStatus().name().toLowerCase());
        node.put("details", taskExecutionResult.getDetails());
        node.putPOJO("resultData", taskExecutionResult.getResultData());
        node.put("completionDateUtc", taskExecutionResult.getCompletionDateUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        return node;
    }

    public static TaskExecutionResult getTaskExecutionResult(JsonNode node)
    {
        return new TaskExecutionResult
            (
                TaskExecutionStatus.valueOf(node.get("status").asText().toUpperCase()),
                node.get("details").asText(),
                getMap(node.get("resultData")),
                LocalDateTime.parse(node.get("completionDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME)
            );
    }

    public static JsonNode newExecutableTask(ExecutableTaskModel executableTask)
    {
        ObjectNode node = newNode();
        node.put("runId", executableTask.getRunId().getId());
        node.put("scheduleId", executableTask.getScheduleId().getId());
        node.set("task", newTask(executableTask.getTask()));
        return node;
    }

    public static ExecutableTaskModel getExecutableTask(JsonNode node)
    {
        return new ExecutableTaskModel
            (
                new RunId(node.get("runId").asText()),
                new ScheduleId(node.get("scheduleId").asText()),
                getTask(node.get("task"))
            );
    }

    public static JsonNode newStartedTask(StartedTaskModel startedTask)
    {
        ObjectNode node = newNode();
        node.put("instanceName", startedTask.getInstanceName());
        node.put("startDateUtc", startedTask.getStartDateUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        return node;
    }

    public static StartedTaskModel getStartedTask(JsonNode node)
    {
        return new StartedTaskModel
        (
            node.get("instanceName").asText(),
            LocalDateTime.parse(node.get("startDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME)
        );
    }

    public static JsonNode newTaskDagContainer(TaskDagContainerModel taskDagContainer)
    {
        ObjectNode node = newNode();
        addId(node, taskDagContainer.getTaskDagId());
        node.set("taskDag", newTaskDag(taskDagContainer.getDag()));
        return node;
    }

    public static TaskDagContainerModel getTaskDagContainer(JsonNode node)
    {
        return new TaskDagContainerModel(new TaskDagId(getId(node)), getTaskDag(node.get("taskDag")));
    }

    public static JsonNode newTaskDagContainers(List<TaskDagContainerModel> containers)
    {
        ArrayNode tab = newArrayNode();
        for ( TaskDagContainerModel taskDagContainer : containers )
        {
            tab.add(newTaskDagContainer(taskDagContainer));
        }
        return tab;
    }

    public static List<TaskDagContainerModel> getTaskDagContainers(JsonNode node)
    {
        List<TaskDagContainerModel> containers = Lists.newArrayList();
        for ( JsonNode n : node )
        {
            containers.add(getTaskDagContainer(n));
        }
        return containers;
    }

    public static void addId(ObjectNode node, Id id)
    {
        node.put("id", id.getId());
    }

    public static String getId(JsonNode node)
    {
        return node.get("id").asText();
    }

    public static Map<String, String> getMap(JsonNode node)
    {
        Map<String, String> map = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while ( fields.hasNext() )
        {
            Map.Entry<String, JsonNode> nodeEntry = fields.next();
            map.put(nodeEntry.getKey(), nodeEntry.getValue().asText());
        }
        return map;
    }

    private JsonSerializer()
    {
    }
}
