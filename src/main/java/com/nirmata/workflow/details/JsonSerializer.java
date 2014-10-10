package com.nirmata.workflow.details;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDag;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public static JsonNode newTask(Task task)
    {
        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        ArrayNode tasks = newArrayNode();
        builder.getTasks().values().forEach(thisTask -> {
            ObjectNode node = newNode();
            node.put("taskId", thisTask.getTaskId().getId());
            node.set("taskType", thisTask.isExecutable() ? newTaskType(thisTask.getTaskType()) : null);
            node.putPOJO("metaData", thisTask.getMetaData());
            node.put("isExecutable", thisTask.isExecutable());

            List<String> childrenTaskIds = thisTask.getChildrenTasks().stream().map(t -> t.getTaskId().getId()).collect(Collectors.toList());
            node.putPOJO("childrenTaskIds", childrenTaskIds);

            tasks.add(node);
        });

        ObjectNode node = newNode();
        node.put("rootTaskId", task.getTaskId().getId());
        node.set("tasks", tasks);
        return node;
    }

    private static class WorkTask
    {
        TaskId taskId;
        TaskType taskType;
        Map<String, String> metaData;
        List<String> childrenTaskIds;
    }

    private static Task buildTask(Map<TaskId, WorkTask> workMap, Map<TaskId, Task> buildMap, String taskIdStr)
    {
        TaskId taskId = new TaskId(taskIdStr);
        Task builtTask = buildMap.get(taskId);
        if ( builtTask != null )
        {
            return builtTask;
        }

        WorkTask task = workMap.get(taskId);
        if ( task == null )
        {
            String message = "Incoherent serialized task. Missing task: " + taskId;
            log.error(message);
            throw new RuntimeException(message);
        }

        List<Task> childrenTasks = task.childrenTaskIds.stream().map(id -> buildTask(workMap, buildMap, id)).collect(Collectors.toList());
        Task newTask = new Task(taskId, task.taskType, childrenTasks, task.metaData);
        buildMap.put(taskId, newTask);
        return newTask;
    }

    public static Task getTask(JsonNode node)
    {
        Map<TaskId, WorkTask> workMap = Maps.newHashMap();
        node.get("tasks").forEach(n -> {
            WorkTask workTask = new WorkTask();
            JsonNode taskTypeNode = n.get("taskType");
            workTask.taskId = new TaskId(n.get("taskId").asText());
            workTask.taskType = ((taskTypeNode != null) && !taskTypeNode.isNull()) ? getTaskType(taskTypeNode) : null;
            workTask.metaData = getMap(n.get("metaData"));
            workTask.childrenTaskIds = Lists.newArrayList();
            n.get("childrenTaskIds").forEach(c -> workTask.childrenTaskIds.add(c.asText()));

            workMap.put(workTask.taskId, workTask);
        });
        String rootTaskId = node.get("rootTaskId").asText();
        return buildTask(workMap, Maps.newHashMap(), rootTaskId);
    }

    public static JsonNode newRunnableTaskDag(RunnableTaskDag runnableTaskDag)
    {
        ArrayNode tab = newArrayNode();
        runnableTaskDag.getDependencies().forEach(taskId -> tab.add(taskId.getId()));

        ObjectNode node = newNode();
        node.put("taskId", runnableTaskDag.getTaskId().getId());
        node.set("dependencies", tab);

        return node;
    }

    public static RunnableTaskDag getRunnableTaskDag(JsonNode node)
    {
        List<TaskId> children = Lists.newArrayList();
        JsonNode childrenNode = node.get("dependencies");
        if ( (childrenNode != null) && (childrenNode.size() > 0) )
        {
            childrenNode.forEach(n -> children.add(new TaskId(n.asText())));
        }

        return new RunnableTaskDag
        (
            new TaskId(node.get("taskId").asText()),
            children
        );
    }

    public static JsonNode newTaskType(TaskType taskType)
    {
        ObjectNode node = newNode();
        node.put("type", taskType.getType());
        node.put("version", taskType.getVersion());
        node.put("isIdempotent", taskType.isIdempotent());
        return node;
    }

    public static TaskType getTaskType(JsonNode node)
    {
        return new TaskType
        (
            node.get("type").asText(),
            node.get("version").asText(),
            node.get("isIdempotent").asBoolean()
        );
    }

    public static JsonNode newExecutableTask(ExecutableTask executableTask)
    {
        ObjectNode node = newNode();
        node.put("runId", executableTask.getRunId().getId());
        node.put("taskId", executableTask.getTaskId().getId());
        node.set("taskType", newTaskType(executableTask.getTaskType()));
        node.putPOJO("metaData", executableTask.getMetaData());
        node.put("isExecutable", executableTask.isExecutable());
        return node;
    }

    public static ExecutableTask getExecutableTask(JsonNode node)
    {
        return new ExecutableTask
        (
            new RunId(node.get("runId").asText()),
            new TaskId(node.get("taskId").asText()),
            getTaskType(node.get("taskType")),
            getMap(node.get("metaData")),
            node.get("isExecutable").asBoolean()
        );
    }

    public static JsonNode newRunnableTask(RunnableTask runnableTask)
    {
        ArrayNode taskDags = newArrayNode();
        runnableTask.getTaskDags().forEach(taskDag -> taskDags.add(newRunnableTaskDag(taskDag)));

        ObjectNode tasks = newNode();
        runnableTask.getTasks().entrySet().forEach(entry -> tasks.set(entry.getKey().getId(), newExecutableTask(entry.getValue())));

        ObjectNode node = newNode();
        node.set("taskDags", taskDags);
        node.set("tasks", tasks);
        node.put("startTime", runnableTask.getStartTime().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("completionTime", runnableTask.getCompletionTime().isPresent() ? runnableTask.getCompletionTime().get().format(DateTimeFormatter.ISO_DATE_TIME) : null);
        return node;
    }

    public static RunnableTask getRunnableTask(JsonNode node)
    {
        List<RunnableTaskDag> taskDags = Lists.newArrayList();
        node.get("taskDags").forEach(n -> taskDags.add(getRunnableTaskDag(n)));

        Map<TaskId, ExecutableTask> tasks = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fields = node.get("tasks").fields();
        while ( fields.hasNext() )
        {
            Map.Entry<String, JsonNode> next = fields.next();
            tasks.put(new TaskId(next.getKey()), getExecutableTask(next.getValue()));
        }

        LocalDateTime startTime = LocalDateTime.parse(node.get("startTime").asText(), DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime completionTime = node.get("completionTime").isNull() ? null : LocalDateTime.parse(node.get("completionTime").asText(), DateTimeFormatter.ISO_DATE_TIME);
        return new RunnableTask(tasks, taskDags, startTime, completionTime);
    }

    public static JsonNode newTaskExecutionResult(TaskExecutionResult taskExecutionResult)
    {
        ObjectNode node = newNode();
        node.put("status", taskExecutionResult.getStatus().name().toLowerCase());
        node.put("message", taskExecutionResult.getMessage());
        node.putPOJO("resultData", taskExecutionResult.getResultData());
        return node;
    }

    public static TaskExecutionResult getTaskExecutionResult(JsonNode node)
    {
        return new TaskExecutionResult
        (
            TaskExecutionStatus.valueOf(node.get("status").asText().toUpperCase()),
            node.get("message").asText(),
            getMap(node.get("resultData"))
        );
    }

    public static JsonNode newStartedTask(StartedTask startedTask)
    {
        ObjectNode node = newNode();
        node.put("instanceName", startedTask.getInstanceName());
        node.put("startDateUtc", startedTask.getStartDateUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        return node;
    }

    public static StartedTask getStartedTask(JsonNode node)
    {
        return new StartedTask
        (
            node.get("instanceName").asText(),
            LocalDateTime.parse(node.get("startDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME)
        );
    }

    public static Map<String, String> getMap(JsonNode node)
    {
        Map<String, String> map = Maps.newHashMap();
        if ( (node != null) && !node.isNull() )
        {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while ( fields.hasNext() )
            {
                Map.Entry<String, JsonNode> nodeEntry = fields.next();
                map.put(nodeEntry.getKey(), nodeEntry.getValue().asText());
            }
        }
        return map;
    }

    private JsonSerializer()
    {
    }
}
