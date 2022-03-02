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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
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
import com.nirmata.workflow.models.TaskType;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.nirmata.workflow.serialization.JsonSerializer.*;

public class TestSerializer {
    private static final Random random = new Random();

    @Test
    public void testRunnableTaskDag() {
        RunnableTaskDag runnableTaskDag = new RunnableTaskDag(new TaskId(), randomTasks());
        JsonNode node = newRunnableTaskDag(runnableTaskDag);
        String str = nodeToString(node);
        System.out.println(str);

        RunnableTaskDag unRunnableTaskDag = getRunnableTaskDag(fromString(str));
        Assert.assertEquals(runnableTaskDag, unRunnableTaskDag);
    }

    @Test
    public void testExecutableTask() {
        ExecutableTask executableTask = new ExecutableTask(new RunId(), new TaskId(), randomTaskType(), randomMap(),
                random.nextBoolean());
        JsonNode node = newExecutableTask(executableTask);
        String str = nodeToString(node);
        System.out.println(str);

        ExecutableTask unExecutableTask = getExecutableTask(fromString(str));
        Assert.assertEquals(executableTask, unExecutableTask);
    }

    @Test
    public void testRunnableTask() {
        Map<TaskId, ExecutableTask> tasks = Stream.generate(() -> "")
                .limit(random.nextInt(3) + 1)
                .collect(Collectors.toMap(s -> new TaskId(), s -> new ExecutableTask(new RunId(), new TaskId(),
                        randomTaskType(), randomMap(), random.nextBoolean())));
        List<RunnableTaskDag> taskDags = Stream.generate(() -> new RunnableTaskDag(new TaskId(), randomTasks()))
                .limit(random.nextInt(3) + 1)
                .collect(Collectors.toList());
        LocalDateTime completionTime = random.nextBoolean() ? LocalDateTime.now() : null;
        RunId parentRunId = random.nextBoolean() ? new RunId() : null;
        RunnableTask runnableTask = new RunnableTask(tasks, taskDags, LocalDateTime.now(), completionTime, parentRunId);
        JsonNode node = newRunnableTask(runnableTask);
        String str = nodeToString(node);
        System.out.println(str);

        RunnableTask unRunnableTask = getRunnableTask(fromString(str));
        Assert.assertEquals(runnableTask, unRunnableTask);
    }

    @Test
    public void testTaskExecutionResult() {
        TaskExecutionResult taskExecutionResult = new TaskExecutionResult(TaskExecutionStatus.SUCCESS,
                Integer.toString(random.nextInt()), randomMap());
        JsonNode node = newTaskExecutionResult(taskExecutionResult);
        String str = nodeToString(node);
        System.out.println(str);

        TaskExecutionResult unTaskExecutionResult = getTaskExecutionResult(fromString(str));
        Assert.assertEquals(taskExecutionResult, unTaskExecutionResult);
    }

    @Test
    public void testStartedTask() {
        StartedTask startedTask = new StartedTask(Integer.toString(random.nextInt()),
                LocalDateTime.now(Clock.systemUTC()), 0);
        JsonNode node = newStartedTask(startedTask);
        String str = nodeToString(node);
        System.out.println(str);

        StartedTask unStartedTask = getStartedTask(fromString(str));
        Assert.assertEquals(startedTask, unStartedTask);
    }

    @Test
    public void testTaskType() {
        TaskType taskType = randomTaskType();
        JsonNode node = newTaskType(taskType);
        String str = nodeToString(node);
        System.out.println(str);

        TaskType unTaskType = getTaskType(fromString(str));
        Assert.assertEquals(taskType, unTaskType);
    }

    @Test
    public void testTask() {
        Task task = randomTask(0);
        JsonNode node = newTask(task);
        String str = nodeToString(node);
        System.out.println(str);

        Task unTask = TaskLoader.load(str);
        Assert.assertEquals(task, unTask);
    }

    @Test
    public void testLoadedTask() throws IOException {
        TaskType taskType = new TaskType("test", "1", true);
        Task task6 = new Task(new TaskId("task6"), taskType);
        Task task5 = new Task(new TaskId("task5"), taskType, Lists.newArrayList(task6), Maps.newHashMap());
        Task task4 = new Task(new TaskId("task4"), taskType, Lists.newArrayList(task6), Maps.newHashMap());
        Task task3 = new Task(new TaskId("task3"), taskType, Lists.newArrayList(task6), Maps.newHashMap());
        Task task2 = new Task(new TaskId("task2"), taskType, Lists.newArrayList(task3, task4, task5),
                Maps.newHashMap());
        Task task1 = new Task(new TaskId("task1"), taskType, Lists.newArrayList(task3, task4, task5),
                Maps.newHashMap());
        Task task = new Task(new TaskId("root"), Lists.newArrayList(task1, task2));

        String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
        Task unTask = TaskLoader.load(json);

        Assert.assertEquals(task, unTask);
    }

    @Test
    public void testWorkflowMessages() {
        // Create constituent objects of workflow message
        Map<TaskId, ExecutableTask> tasks = Stream.generate(() -> "")
                .limit(random.nextInt(3) + 1)
                .collect(Collectors.toMap(s -> new TaskId(), s -> new ExecutableTask(new RunId(), new TaskId(),
                        randomTaskType(), randomMap(), random.nextBoolean())));
        List<RunnableTaskDag> taskDags = Stream.generate(() -> new RunnableTaskDag(new TaskId(), randomTasks()))
                .limit(random.nextInt(3) + 1)
                .collect(Collectors.toList());
        LocalDateTime completionTime = random.nextBoolean() ? LocalDateTime.now() : null;
        RunId parentRunId = random.nextBoolean() ? new RunId() : null;
        RunnableTask runnableTask = new RunnableTask(tasks, taskDags, LocalDateTime.now(), completionTime, parentRunId);
        TaskExecutionResult taskExecutionResult = new TaskExecutionResult(TaskExecutionStatus.SUCCESS,
                Integer.toString(random.nextInt()), randomMap());

        // Assert serialize-deserialize for all workflow message types
        WorkflowMessage msg, unMsg;
        String nodeStr;
        msg = new WorkflowMessage();
        nodeStr = nodeToString(newWorkflowMessage(msg));
        unMsg = getWorkflowMessage(fromString(nodeStr));

        Assert.assertEquals(msg, unMsg);

        msg = new WorkflowMessage(runnableTask);
        nodeStr = nodeToString(newWorkflowMessage(msg));
        unMsg = getWorkflowMessage(fromString(nodeStr));
        Assert.assertEquals(msg, unMsg);

        msg = new WorkflowMessage(runnableTask, true);
        nodeStr = nodeToString(newWorkflowMessage(msg));
        unMsg = getWorkflowMessage(fromString(nodeStr));
        Assert.assertEquals(msg, unMsg);

        msg = new WorkflowMessage(new TaskId(), taskExecutionResult);
        nodeStr = nodeToString(newWorkflowMessage(msg));
        unMsg = getWorkflowMessage(fromString(nodeStr));
        Assert.assertEquals(msg, unMsg);

    }

    @Test
    public void testJsonSerializerMapper() {
        JsonSerializerMapper mapper = new JsonSerializerMapper();
        Task task = randomTask(0);
        JsonNode node = mapper.make(task);
        String str = nodeToString(node);
        System.out.println(str);

        Task unTask = mapper.get(fromString(str), Task.class);
        Assert.assertEquals(task, unTask);
    }

    @Test
    public void testAlternateSerializers() {
        JDKSerializer serializer = new JDKSerializer();

        StartedTask startedTask = new StartedTask(Integer.toString(random.nextInt()),
                LocalDateTime.now(Clock.systemUTC()), 0);
        byte[] bytes = serializer.serialize(startedTask);
        StartedTask unStartedTask = serializer.deserialize(bytes, StartedTask.class);
        Assert.assertEquals(startedTask, unStartedTask);
    }

    private Task randomTask(int index) {
        List<Task> childrenTasks = Lists.newArrayList();
        boolean shouldHaveChildren = (index == 0) || ((index < 2) && random.nextBoolean());
        int childrenQty = shouldHaveChildren ? random.nextInt(5) : 0;
        IntStream.range(0, childrenQty).forEach(i -> childrenTasks.add(randomTask(index + 1)));
        return new Task(new TaskId(), randomTaskType(), childrenTasks, randomMap());
    }

    private TaskType randomTaskType() {
        return new TaskType(Integer.toString(random.nextInt()), Integer.toHexString(random.nextInt()),
                random.nextBoolean());
    }

    private Map<String, String> randomMap() {
        return Stream.generate(random::nextInt)
                .limit(random.nextInt(3) + 1)
                .collect(Collectors.toMap(n -> Integer.toString(n * 4), n -> Integer.toString(n * 2)));
    }

    private Collection<TaskId> randomTasks() {
        return Stream.generate(TaskId::new)
                .limit(random.nextInt(10) + 1)
                .collect(Collectors.toList());
    }
}
