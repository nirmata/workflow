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
import com.nirmata.workflow.details.internalmodels.WorkflowMessage;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskMode;
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

class JsonSerializer {
    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    static ObjectNode newNode() {
        return mapper.createObjectNode();
    }

    static ArrayNode newArrayNode() {
        return mapper.createArrayNode();
    }

    static ObjectMapper getMapper() {
        return mapper;
    }

    static byte[] toBytes(JsonNode node) {
        return nodeToString(node).getBytes();
    }

    static String nodeToString(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            log.error("mapper.writeValueAsString", e);
            throw new RuntimeException(e);
        }
    }

    static JsonNode fromBytes(byte[] bytes) {
        return fromString(new String(bytes));
    }

    static JsonNode fromString(String str) {
        try {
            return mapper.readTree(str);
        } catch (IOException e) {
            log.error("reading JSON: " + str, e);
            throw new RuntimeException(e);
        }
    }

    static JsonNode newTask(Task task) {
        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        ArrayNode tasks = newArrayNode();
        builder.getTasks().values().forEach(thisTask -> {
            ObjectNode node = newNode();
            node.put("taskId", thisTask.getTaskId().getId());
            node.set("taskType", thisTask.isExecutable() ? newTaskType(thisTask.getTaskType()) : null);
            node.putPOJO("metaData", thisTask.getMetaData());
            node.put("isExecutable", thisTask.isExecutable());

            List<String> childrenTaskIds = thisTask.getChildrenTasks().stream().map(t -> t.getTaskId().getId())
                    .collect(Collectors.toList());
            node.putPOJO("childrenTaskIds", childrenTaskIds);

            tasks.add(node);
        });

        ObjectNode node = newNode();
        node.put("rootTaskId", task.getTaskId().getId());
        node.set("tasks", tasks);
        return node;
    }

    private static class WorkTask {
        TaskId taskId;
        TaskType taskType;
        Map<String, String> metaData;
        List<String> childrenTaskIds;
    }

    private static Task buildTask(Map<TaskId, WorkTask> workMap, Map<TaskId, Task> buildMap, String taskIdStr) {
        TaskId taskId = new TaskId(taskIdStr);
        Task builtTask = buildMap.get(taskId);
        if (builtTask != null) {
            return builtTask;
        }

        WorkTask task = workMap.get(taskId);
        if (task == null) {
            String message = "Incoherent serialized task. Missing task: " + taskId;
            log.error(message);
            throw new RuntimeException(message);
        }

        List<Task> childrenTasks = task.childrenTaskIds.stream().map(id -> buildTask(workMap, buildMap, id))
                .collect(Collectors.toList());
        Task newTask = new Task(taskId, task.taskType, childrenTasks, task.metaData);
        buildMap.put(taskId, newTask);
        return newTask;
    }

    static Task getTask(JsonNode node) {
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

    static JsonNode newRunnableTaskDag(RunnableTaskDag runnableTaskDag) {
        ArrayNode tab = newArrayNode();
        runnableTaskDag.getDependencies().forEach(taskId -> tab.add(taskId.getId()));

        ObjectNode node = newNode();
        node.put("taskId", runnableTaskDag.getTaskId().getId());
        node.set("dependencies", tab);

        return node;
    }

    static RunnableTaskDag getRunnableTaskDag(JsonNode node) {
        List<TaskId> children = Lists.newArrayList();
        JsonNode childrenNode = node.get("dependencies");
        if ((childrenNode != null) && (childrenNode.size() > 0)) {
            childrenNode.forEach(n -> children.add(new TaskId(n.asText())));
        }

        return new RunnableTaskDag(
                new TaskId(node.get("taskId").asText()),
                children);
    }

    static JsonNode newTaskType(TaskType taskType) {
        ObjectNode node = newNode();
        node.put("type", taskType.getType());
        node.put("version", taskType.getVersion());
        node.put("isIdempotent", taskType.isIdempotent());
        node.put("mode", taskType.getMode().getCode());
        return node;
    }

    static TaskType getTaskType(JsonNode node) {
        TaskMode taskMode = node.has("mode") ? TaskMode.fromCode(node.get("mode").intValue()) : TaskMode.STANDARD; // for
                                                                                                                   // backward
                                                                                                                   // compatability
        return new TaskType(
                node.get("type").asText(),
                node.get("version").asText(),
                node.get("isIdempotent").asBoolean(),
                taskMode);
    }

    static JsonNode newExecutableTask(ExecutableTask executableTask) {
        ObjectNode node = newNode();
        node.put("runId", executableTask.getRunId().getId());
        node.put("taskId", executableTask.getTaskId().getId());
        node.set("taskType", newTaskType(executableTask.getTaskType()));
        node.putPOJO("metaData", executableTask.getMetaData());
        node.put("isExecutable", executableTask.isExecutable());
        return node;
    }

    static ExecutableTask getExecutableTask(JsonNode node) {
        return new ExecutableTask(
                new RunId(node.get("runId").asText()),
                new TaskId(node.get("taskId").asText()),
                getTaskType(node.get("taskType")),
                getMap(node.get("metaData")),
                node.get("isExecutable").asBoolean());
    }

    static JsonNode newWorkflowMessage(WorkflowMessage workflowMsg) {
        ObjectNode node = newNode();
        node.put("msgType", workflowMsg.getMsgType().toString());
        node.put("isRetry", workflowMsg.isRetry());
        node.set("runnableTask",
                workflowMsg.getRunnableTask().isPresent() ? newRunnableTask(workflowMsg.getRunnableTask().get())
                        : null);
        node.set("taskExecResult",
                workflowMsg.getTaskExecResult().isPresent()
                        ? newTaskExecutionResult(workflowMsg.getTaskExecResult().get())
                        : null);

        node.put("taskId",
                workflowMsg.getTaskId().isPresent() ? workflowMsg.getTaskId().get().getId() : null);
        node.set("taskExecResult",
                workflowMsg.getTaskExecResult().isPresent()
                        ? newTaskExecutionResult(workflowMsg.getTaskExecResult().get())
                        : null);
        return node;
    }

    static WorkflowMessage getWorkflowMessage(JsonNode node) {
        WorkflowMessage.MsgType msgType = WorkflowMessage.MsgType.valueOf(node.get("msgType").asText());
        boolean isRetry = node.get("isRetry").asBoolean();
        RunnableTask rt = (msgType == WorkflowMessage.MsgType.TASK) ? getRunnableTask(node.get("runnableTask")) : null;
        TaskId taskId = (msgType == WorkflowMessage.MsgType.TASKRESULT) ? new TaskId(node.get("taskId").asText())
                : null;
        TaskExecutionResult res = (msgType == WorkflowMessage.MsgType.TASKRESULT)
                ? getTaskExecutionResult(node.get("taskExecResult"))
                : null;

        WorkflowMessage msg;
        switch (msgType) {
            case TASK:
                msg = new WorkflowMessage(rt, isRetry);
                break;
            case TASKRESULT:
                msg = new WorkflowMessage(taskId, res);
                break;
            case CANCEL:
                msg = new WorkflowMessage();
                break;
            default:
                throw new RuntimeException("Unhandled workflow message " + msgType);
        }
        return msg;
    }

    static JsonNode newRunnableTask(RunnableTask runnableTask) {
        ArrayNode taskDags = newArrayNode();
        runnableTask.getTaskDags().forEach(taskDag -> taskDags.add(newRunnableTaskDag(taskDag)));

        ObjectNode tasks = newNode();
        runnableTask.getTasks().forEach((key, value) -> tasks.set(key.getId(), newExecutableTask(value)));

        ObjectNode node = newNode();
        node.set("taskDags", taskDags);
        node.set("tasks", tasks);
        node.put("startTimeUtc", runnableTask.getStartTimeUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("completionTimeUtc",
                runnableTask.getCompletionTimeUtc().isPresent()
                        ? runnableTask.getCompletionTimeUtc().get().format(DateTimeFormatter.ISO_DATE_TIME)
                        : null);
        node.put("parentRunId",
                runnableTask.getParentRunId().isPresent() ? runnableTask.getParentRunId().get().getId() : null);
        return node;
    }

    static RunnableTask getRunnableTask(JsonNode node) {
        List<RunnableTaskDag> taskDags = Lists.newArrayList();
        node.get("taskDags").forEach(n -> taskDags.add(getRunnableTaskDag(n)));

        Map<TaskId, ExecutableTask> tasks = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fields = node.get("tasks").fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
            tasks.put(new TaskId(next.getKey()), getExecutableTask(next.getValue()));
        }

        LocalDateTime startTime = LocalDateTime.parse(node.get("startTimeUtc").asText(),
                DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime completionTime = node.get("completionTimeUtc").isNull() ? null
                : LocalDateTime.parse(node.get("completionTimeUtc").asText(), DateTimeFormatter.ISO_DATE_TIME);
        RunId parentRunId = node.get("parentRunId").isNull() ? null : new RunId(node.get("parentRunId").asText());
        return new RunnableTask(tasks, taskDags, startTime, completionTime, parentRunId);
    }

    static JsonNode newTaskExecutionResult(TaskExecutionResult taskExecutionResult) {
        ObjectNode node = newNode();
        node.put("status", taskExecutionResult.getStatus().name().toLowerCase());
        node.put("message", taskExecutionResult.getMessage());
        node.putPOJO("resultData", taskExecutionResult.getResultData());
        node.put("subTaskRunId",
                taskExecutionResult.getSubTaskRunId().isPresent() ? taskExecutionResult.getSubTaskRunId().get().getId()
                        : null);
        node.put("completionTimeUtc",
                taskExecutionResult.getCompletionTimeUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        return node;
    }

    static TaskExecutionResult getTaskExecutionResult(JsonNode node) {
        JsonNode subTaskRunIdNode = node.get("subTaskRunId");
        return new TaskExecutionResult(
                TaskExecutionStatus.valueOf(node.get("status").asText().toUpperCase()),
                node.get("message").asText(),
                getMap(node.get("resultData")),
                ((subTaskRunIdNode != null) && !subTaskRunIdNode.isNull()) ? new RunId(subTaskRunIdNode.asText())
                        : null,
                LocalDateTime.parse(node.get("completionTimeUtc").asText(), DateTimeFormatter.ISO_DATE_TIME));
    }

    static JsonNode newStartedTask(StartedTask startedTask) {
        ObjectNode node = newNode();
        node.put("instanceName", startedTask.getInstanceName());
        node.put("startDateUtc", startedTask.getStartDateUtc().format(DateTimeFormatter.ISO_DATE_TIME));
        node.put("progress", startedTask.getProgress());
        return node;
    }

    static StartedTask getStartedTask(JsonNode node) {
        return new StartedTask(
                node.get("instanceName").asText(),
                LocalDateTime.parse(node.get("startDateUtc").asText(), DateTimeFormatter.ISO_DATE_TIME),
                node.has("progress") ? node.get("progress").asInt() : 0);
    }

    static Map<String, String> getMap(JsonNode node) {
        Map<String, String> map = Maps.newHashMap();
        if ((node != null) && !node.isNull()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> nodeEntry = fields.next();
                map.put(nodeEntry.getKey(), nodeEntry.getValue().asText());
            }
        }
        return map;
    }

    private JsonSerializer() {
    }
}
