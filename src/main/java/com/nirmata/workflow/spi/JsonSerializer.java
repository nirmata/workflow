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

    public static byte[] toBytes(ObjectNode node)
    {
        return nodeToString(node).getBytes();
    }

    public static String nodeToString(ObjectNode node)
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

    public static ObjectNode addScheduleExecution(ObjectNode node, ScheduleExecutionModel scheduleExecution)
    {
        ObjectNode scheduleExecutionNode = newNode();
        scheduleExecutionNode.put("scheduleId", scheduleExecution.getScheduleId().getId());
        scheduleExecutionNode.put("lastExecutionStartUtc", Clock.dateToString(scheduleExecution.getLastExecutionStartUtc()));
        scheduleExecutionNode.put("lastExecutionEndUtc", Clock.dateToString(scheduleExecution.getLastExecutionEndUtc()));
        scheduleExecutionNode.put("executionQty", scheduleExecution.getExecutionQty());
        node.set("scheduleExecution", scheduleExecutionNode);
        return node;
    }

    public static ScheduleExecutionModel getScheduleExecution(JsonNode node)
    {
        JsonNode scheduleExecutionNode = node.get("scheduleExecution");
        return new ScheduleExecutionModel
        (
            new ScheduleId(scheduleExecutionNode.get("scheduleId").asText()),
            Clock.dateFromString(scheduleExecutionNode.get("lastExecutionStartUtc").asText()),
            Clock.dateFromString(scheduleExecutionNode.get("lastExecutionEndUtc").asText()),
            scheduleExecutionNode.get("executionQty").asInt()
        );
    }

    public static ObjectNode addWorkflows(ObjectNode node, List<WorkflowModel> workflows)
    {
        ArrayNode tab = newArrayNode();
        for ( WorkflowModel workflow : workflows )
        {
            ObjectNode workflowNode = newNode();
            addWorkflow(workflowNode, workflow);
            tab.add(workflowNode);
        }
        node.set("workflows", tab);
        return node;
    }

    public static List<WorkflowModel> getWorkflows(JsonNode node)
    {
        ImmutableList.Builder<WorkflowModel> builder = ImmutableList.builder();
        JsonNode tab = node.get("workflows");
        Iterator<JsonNode> elements = tab.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            builder.add(getWorkflow(next));
        }
        return builder.build();
    }

    public static ObjectNode addWorkflow(ObjectNode node, WorkflowModel workflow)
    {
        ObjectNode workflowNode = newNode();
        addId(workflowNode, workflow.getWorkflowId());
        workflowNode.put("name", workflow.getName());
        addTaskSet(workflowNode, workflow.getTasks());
        node.set("workflow", workflowNode);
        return node;
    }

    public static WorkflowModel getWorkflow(JsonNode node)
    {
        JsonNode workflowNode = node.get("workflow");
        return new WorkflowModel
        (
            new WorkflowId(getId(workflowNode)),
            workflowNode.get("name").asText(),
            getTaskSet(workflowNode)
        );
    }

    public static ObjectNode addSchedules(ObjectNode node, List<ScheduleModel> schedules)
    {
        ArrayNode tab = newArrayNode();
        for ( ScheduleModel schedule : schedules )
        {
            ObjectNode scheduleNode = newNode();
            addSchedule(scheduleNode, schedule);
            tab.add(scheduleNode);
        }
        node.set("schedules", tab);
        return node;
    }

    public static List<ScheduleModel> getSchedules(JsonNode node)
    {
        ImmutableList.Builder<ScheduleModel> builder = ImmutableList.builder();
        JsonNode tab = node.get("schedules");
        Iterator<JsonNode> elements = tab.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            builder.add(getSchedule(next));
        }
        return builder.build();
    }

    public static ObjectNode addSchedule(ObjectNode node, ScheduleModel schedule)
    {
        ObjectNode scheduleNode = newNode();
        addId(scheduleNode, schedule.getScheduleId());
        addRepetition(scheduleNode, schedule.getRepetition());
        scheduleNode.put("workflowId", schedule.getWorkflowId().getId());
        node.set("schedule", scheduleNode);
        return node;
    }

    public static ScheduleModel getSchedule(JsonNode node)
    {
        JsonNode scheduleNode = node.get("schedule");
        return new ScheduleModel
        (
            new ScheduleId(getId(scheduleNode)),
            new WorkflowId(scheduleNode.get("workflowId").asText()), getRepetition(scheduleNode)
        );
    }

    public static ObjectNode addTaskSet(ObjectNode node, TaskSets taskSets)
    {
        ArrayNode tab = newArrayNode();
        for ( List<TaskId> tasks : taskSets )
        {
            ArrayNode tasksTab = newArrayNode();
            for ( TaskId taskId : tasks )
            {
                ObjectNode idNode = newNode();
                addId(idNode, taskId);
                tasksTab.add(idNode);
            }
            tab.add(tasksTab);
        }
        node.set("taskSet", tab);
        return node;
    }

    public static TaskSets getTaskSet(JsonNode node)
    {
        List<List<TaskId>> tasks = Lists.newArrayList();
        JsonNode tab = node.get("taskSet");
        Iterator<JsonNode> elements = tab.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            List<TaskId> thisSet = Lists.newArrayList();
            for ( JsonNode idNode : next )
            {
                thisSet.add(new TaskId(getId(idNode)));
            }
            tasks.add(thisSet);
        }
        return new TaskSets(tasks);
    }

    public static ObjectNode addTasks(ObjectNode node, Collection<TaskModel> tasks)
    {
        ArrayNode tab = newArrayNode();
        for ( TaskModel task : tasks )
        {
            ObjectNode taskNode = newNode();
            addTask(taskNode, task);
            tab.add(taskNode);
        }
        node.set("tasks", tab);
        return node;
    }

    public static List<TaskModel> getTasks(JsonNode node)
    {
        ImmutableList.Builder<TaskModel> builder = ImmutableList.builder();
        JsonNode tab = node.get("tasks");
        Iterator<JsonNode> elements = tab.elements();
        while ( elements.hasNext() )
        {
            JsonNode next = elements.next();
            builder.add(getTask(next));
        }
        return builder.build();
    }

    public static ObjectNode addTask(ObjectNode node, TaskModel task)
    {
        ObjectNode taskNode = newNode();
        addId(taskNode, task.getTaskId());
        taskNode.put("name", task.getName());
        taskNode.put("code", task.getTaskExecutionCode());
        taskNode.put("isIdempotent", task.isIdempotent());
        taskNode.putPOJO("meta", task.getMetaData());
        node.set("task", taskNode);
        return node;
    }

    public static TaskModel getTask(JsonNode node)
    {
        JsonNode taskNode = node.get("task");
        return new TaskModel
        (
            new TaskId(getId(taskNode)),
            taskNode.get("name").asText(),
            taskNode.get("code").asText(),
            taskNode.get("isIdempotent").asBoolean(),
            getMap(taskNode.get("meta"))
        );
    }

    public static ObjectNode addRepetition(ObjectNode node, RepetitionModel repetition)
    {
        ObjectNode repetitionNode = newNode();
        repetitionNode.put("duration", repetition.getDuration().toString());
        repetitionNode.put("type", repetition.getType().name());
        repetitionNode.put("qty", repetition.getQty());
        node.set("repetition", repetitionNode);
        return node;
    }

    public static RepetitionModel getRepetition(JsonNode node)
    {
        JsonNode repetitionNode = node.get("repetition");
        return new RepetitionModel
        (
            Duration.valueOf(repetitionNode.get("duration").asText()),
            RepetitionModel.Type.valueOf(repetitionNode.get("type").asText().toUpperCase()),
            repetitionNode.get("qty").asInt()
        );
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
