package com.nirmata.workflow.details;

import com.fasterxml.jackson.databind.JsonNode;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.RunnableTaskDag;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nirmata.workflow.details.JsonSerializer.*;

public class TestJsonSerializer
{
    private static final Random random = new Random();

    @Test
    public void testRunnableTaskDag()
    {
        RunnableTaskDag runnableTaskDag = new RunnableTaskDag(new TaskId(), randomTasks());
        JsonNode node = newRunnableTaskDag(runnableTaskDag);
        String str = nodeToString(node);
        System.out.println(str);

        RunnableTaskDag unRunnableTaskDag = getRunnableTaskDag(fromString(str));
        Assert.assertEquals(runnableTaskDag, unRunnableTaskDag);
    }

    @Test
    public void testExecutableTask()
    {
        ExecutableTask executableTask = new ExecutableTask(new TaskId(), randomTaskType(), randomMap(), random.nextBoolean());
        JsonNode node = newExecutableTask(executableTask);
        String str = nodeToString(node);
        System.out.println(str);

        ExecutableTask unExecutableTask = getExecutableTask(fromString(str));
        Assert.assertEquals(executableTask, unExecutableTask);
    }

    @Test
    public void testRunnableTask()
    {
        Map<TaskId, ExecutableTask> tasks = Stream.generate(() -> "")
            .limit(random.nextInt(3) + 1)
            .collect(Collectors.toMap(s -> new TaskId(), s -> new ExecutableTask(new TaskId(), randomTaskType(), randomMap(), random.nextBoolean())))
            ;
        List<RunnableTaskDag> taskDags = Stream.generate(() -> new RunnableTaskDag(new TaskId(), randomTasks()))
            .limit(random.nextInt(3) + 1)
            .collect(Collectors.toList())
            ;
        RunnableTask runnableTask = new RunnableTask(tasks, taskDags, LocalDateTime.now(), null);
        JsonNode node = newRunnableTask(runnableTask);
        String str = nodeToString(node);
        System.out.println(str);

        RunnableTask unRunnableTask = getRunnableTask(fromString(str));
        Assert.assertEquals(runnableTask, unRunnableTask);
    }

    @Test
    public void testTaskExecutionResult()
    {
        TaskExecutionResult taskExecutionResult = new TaskExecutionResult(TaskExecutionStatus.SUCCESS, Integer.toString(random.nextInt()), randomMap());
        JsonNode node = newTaskExecutionResult(taskExecutionResult);
        String str = nodeToString(node);
        System.out.println(str);

        TaskExecutionResult unTaskExecutionResult = getTaskExecutionResult(fromString(str));
        Assert.assertEquals(taskExecutionResult, unTaskExecutionResult);
    }

    @Test
    public void testStartedTask()
    {
        StartedTask startedTask = new StartedTask(Integer.toString(random.nextInt()), LocalDateTime.now(Clock.systemUTC()));
        JsonNode node = newStartedTask(startedTask);
        String str = nodeToString(node);
        System.out.println(str);

        StartedTask unStartedTask = getStartedTask(fromString(str));
        Assert.assertEquals(startedTask, unStartedTask);
    }

    @Test
    public void testTaskType()
    {
        TaskType taskType = randomTaskType();
        JsonNode node = newTaskType(taskType);
        String str = nodeToString(node);
        System.out.println(str);

        TaskType unTaskType = getTaskType(fromString(str));
        Assert.assertEquals(taskType, unTaskType);
    }

    private TaskType randomTaskType()
    {
        return new TaskType(Integer.toString(random.nextInt()), Integer.toHexString(random.nextInt()), random.nextBoolean());
    }

    private Map<String, String> randomMap()
    {
        return Stream.generate(random::nextInt)
            .limit(random.nextInt(3) + 1)
            .collect(Collectors.toMap(n -> Integer.toString(n * 4), n -> Integer.toString(n * 2)));
    }

    private Collection<TaskId> randomTasks()
    {
        return Stream.generate(TaskId::new)
            .limit(random.nextInt(10) + 1)
            .collect(Collectors.toList());
    }
}
