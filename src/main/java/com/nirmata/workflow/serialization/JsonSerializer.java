/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.RunnableTaskDagBuilder;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDag;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class JsonSerializer
{
    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter ISO_UTC = ISODateTimeFormat.dateTime().withZoneUTC();

    static ObjectNode newNode()
    {
        return mapper.createObjectNode();
    }

    static ArrayNode newArrayNode()
    {
        return mapper.createArrayNode();
    }

    static ObjectMapper getMapper()
    {
        return mapper;
    }

    static byte[] toBytes(JsonNode node)
    {
        return nodeToString(node).getBytes();
    }

    static String nodeToString(JsonNode node)
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

    static JsonNode fromBytes(byte[] bytes)
    {
        return fromString(new String(bytes));
    }

    static JsonNode fromString(String str)
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

    static JsonNode newTask(Task task)
    {
        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        ArrayNode tasks = newArrayNode();
        for ( Task thisTask : builder.getTasks().values() )
        {
            ObjectNode node = newNode();
            node.put("taskId", thisTask.getTaskId().getId());
            node.set("taskType", thisTask.isExecutable() ? newTaskType(thisTask.getTaskType()) : null);
            node.putPOJO("metaData", thisTask.getMetaData());
            node.put("isExecutable", thisTask.isExecutable());

            List<String> childrenTaskIds = new ArrayList<>(thisTask.getChildrenTasks().size());
            for ( Task t : thisTask.getChildrenTasks() )
            {
                childrenTaskIds.add(t.getTaskId().getId());
            }
            node.putPOJO("childrenTaskIds", childrenTaskIds);

            tasks.add(node);
        }

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

        List<Task> childrenTasks = new ArrayList<>(task.childrenTaskIds.size());
        for ( String id : task.childrenTaskIds )
        {
            childrenTasks.add(buildTask(workMap, buildMap, id));
        }
        Task newTask = new Task(taskId, task.taskType, childrenTasks, task.metaData);
        buildMap.put(taskId, newTask);
        return newTask;
    }

    static Task getTask(JsonNode node)
    {
        Map<TaskId, WorkTask> workMap = Maps.newLinkedHashMap();
        for ( JsonNode n: node.get("tasks") )
        {
            WorkTask workTask = new WorkTask();
            JsonNode taskTypeNode = n.get("taskType");
            workTask.taskId = new TaskId(n.get("taskId").asText());
            workTask.taskType = ((taskTypeNode != null) && !taskTypeNode.isNull()) ? getTaskType(taskTypeNode) : null;
            workTask.metaData = getMap(n.get("metaData"));
            workTask.childrenTaskIds = Lists.newArrayList();
            for ( JsonNode c : n.get("childrenTaskIds") )
            {
                workTask.childrenTaskIds.add(c.asText());
            }

            workMap.put(workTask.taskId, workTask);
        }
        String rootTaskId = node.get("rootTaskId").asText();
        return buildTask(workMap, Maps.<TaskId, Task>newLinkedHashMap(), rootTaskId);
    }

    static JsonNode newRunnableTaskDag(RunnableTaskDag runnableTaskDag)
    {
        ArrayNode tab = newArrayNode();
        for ( TaskId taskId: runnableTaskDag.getDependencies() )
        {
            tab.add(taskId.getId());
        }

        ObjectNode node = newNode();
        node.put("taskId", runnableTaskDag.getTaskId().getId());
        node.set("dependencies", tab);

        return node;
    }

    static RunnableTaskDag getRunnableTaskDag(JsonNode node)
    {
        List<TaskId> children = Lists.newArrayList();
        JsonNode childrenNode = node.get("dependencies");
        if ( (childrenNode != null) && (childrenNode.size() > 0) )
        {
            for (JsonNode n : childrenNode)
            {
                children.add(new TaskId(n.asText()));
            }
        }

        return new RunnableTaskDag
        (
            new TaskId(node.get("taskId").asText()),
            children
        );
    }

    static JsonNode newTaskType(TaskType taskType)
    {
        ObjectNode node = newNode();
        node.put("type", taskType.getType());
        node.put("version", taskType.getVersion());
        node.put("isIdempotent", taskType.isIdempotent());
        node.put("mode", taskType.getMode().getCode());
        return node;
    }

    static TaskType getTaskType(JsonNode node)
    {
        TaskMode taskMode = node.has("mode") ? TaskMode.fromCode(node.get("mode").intValue()) : TaskMode.STANDARD; // for backward compatability
        return new TaskType
        (
            node.get("type").asText(),
            node.get("version").asText(),
            node.get("isIdempotent").asBoolean(),
            taskMode
        );
    }

    static JsonNode newExecutableTask(ExecutableTask executableTask)
    {
        ObjectNode node = newNode();
        node.put("runId", executableTask.getRunId().getId());
        node.put("taskId", executableTask.getTaskId().getId());
        node.set("taskType", newTaskType(executableTask.getTaskType()));
        node.putPOJO("metaData", executableTask.getMetaData());
        node.put("isExecutable", executableTask.isExecutable());
        return node;
    }

    static ExecutableTask getExecutableTask(JsonNode node)
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

    static JsonNode newRunnableTask(RunnableTask runnableTask)
    {
        ArrayNode taskDags = newArrayNode();
        for ( RunnableTaskDag taskDag: runnableTask.getTaskDags() )
        {
            taskDags.add(newRunnableTaskDag(taskDag));
        }

        ObjectNode tasks = newNode();
        for ( Map.Entry<TaskId, ExecutableTask> entry: runnableTask.getTasks().entrySet() )
        {
            tasks.set(entry.getKey().getId(), newExecutableTask(entry.getValue()));
        }

        ObjectNode node = newNode();
        node.set("taskDags", taskDags);
        node.set("tasks", tasks);
        node.put("startTimeUtc", writeIsoUtc(runnableTask.getStartTimeUtc()));
        node.put("completionTimeUtc", writeIsoUtc(runnableTask.getCompletionTimeUtc()));
        node.put("parentRunId", runnableTask.getParentRunId() != null ? runnableTask.getParentRunId().getId() : null);
        return node;
    }

    static RunnableTask getRunnableTask(JsonNode node)
    {
        List<RunnableTaskDag> taskDags = Lists.newArrayList();
        for ( JsonNode n : node.get("taskDags") )
        {
            taskDags.add(getRunnableTaskDag(n));
        }

        Map<TaskId, ExecutableTask> tasks = Maps.newLinkedHashMap();
        Iterator<Map.Entry<String, JsonNode>> fields = node.get("tasks").fields();
        while ( fields.hasNext() )
        {
            Map.Entry<String, JsonNode> next = fields.next();
            tasks.put(new TaskId(next.getKey()), getExecutableTask(next.getValue()));
        }

        LocalDateTime startTime = readIsoUtc(node.get("startTimeUtc"));
        LocalDateTime completionTime = readIsoUtc(node.get("completionTimeUtc"));
        RunId parentRunId = node.get("parentRunId").isNull() ? null : new RunId(node.get("parentRunId").asText());
        return new RunnableTask(tasks, taskDags, startTime, completionTime, parentRunId);
    }

    static JsonNode newTaskExecutionResult(TaskExecutionResult taskExecutionResult)
    {
        ObjectNode node = newNode();
        node.put("status", taskExecutionResult.getStatus().name().toLowerCase());
        node.put("message", taskExecutionResult.getMessage());
        node.putPOJO("resultData", taskExecutionResult.getResultData());
        node.put("subTaskRunId", taskExecutionResult.getSubTaskRunId() != null ? taskExecutionResult.getSubTaskRunId().getId() : null);
        node.put("completionTimeUtc", writeIsoUtc(taskExecutionResult.getCompletionTimeUtc()));
        return node;
    }

    static TaskExecutionResult getTaskExecutionResult(JsonNode node)
    {
        JsonNode subTaskRunIdNode = node.get("subTaskRunId");
        return new TaskExecutionResult
        (
            TaskExecutionStatus.valueOf(node.get("status").asText().toUpperCase()),
            node.get("message").asText(),
            getMap(node.get("resultData")),
            ((subTaskRunIdNode != null) && !subTaskRunIdNode.isNull()) ? new RunId(subTaskRunIdNode.asText()) : null, readIsoUtc(node.get("completionTimeUtc"))
        );
    }

    static JsonNode newStartedTask(StartedTask startedTask)
    {
        ObjectNode node = newNode();
        node.put("instanceName", startedTask.getInstanceName());
        node.put("startDateUtc", writeIsoUtc(startedTask.getStartDateUtc()));
        return node;
    }

    static StartedTask getStartedTask(JsonNode node)
    {
        return new StartedTask
        (
            node.get("instanceName").asText(),
            readIsoUtc(node.get("startDateUtc"))
        );
    }

    static Map<String, String> getMap(JsonNode node)
    {
        Map<String, String> map = Maps.newLinkedHashMap();
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

    private static LocalDateTime readIsoUtc(JsonNode node) {
        if (node.isNull())
        {
            return null;
        }
        String text = node.asText(); // Joda is strict wrt the timezone being present.
        if (Character.isDigit(text.charAt(text.length() - 1)))
        {
            text += 'Z';
        }
        return ISO_UTC.parseLocalDateTime(text);
    }

    private static String writeIsoUtc(LocalDateTime date) {
        return date != null ? ISO_UTC.print(date) : null;
    }

    private JsonSerializer()
    {
    }
}
