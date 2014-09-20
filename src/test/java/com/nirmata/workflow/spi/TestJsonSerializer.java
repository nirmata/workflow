package com.nirmata.workflow.spi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.models.*;
import io.airlift.units.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.nirmata.workflow.details.InternalJsonSerializer.newDenormalizedWorkflow;
import static com.nirmata.workflow.details.InternalJsonSerializer.getDenormalizedWorkflow;
import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TestJsonSerializer
{
    private static final Random random = new Random();

    @Test
    public void testId()
    {
        ObjectNode node = newNode();
        Id id = new Id(){};
        addId(node, id);
        String str = nodeToString(node);
        System.out.println(str);

        String unId = getId(fromString(str));
        Assert.assertEquals(unId, id.getId());
    }

    @Test
    public void testTask()
    {
        TaskModel task = makeTask();

        JsonNode node = newTask(task);
        String str = nodeToString(node);
        System.out.println(str);

        TaskModel unTask = getTask(fromString(str));
        Assert.assertEquals(task, unTask);
    }

    public TaskModel makeTask()
    {
        return makeTask(new TaskId());
    }

    public TaskModel makeTask(TaskId taskId)
    {
        Map<String, String> metaData = Maps.newHashMap();
        if ( random.nextBoolean() )
        {
            int qty = random.nextInt(25);
            for ( int i = 0; i < qty; ++i  )
            {
                metaData.put(Integer.toString(i), "" + random.nextInt());
            }
        }
        return new TaskModel(taskId, "test" + random.nextDouble(), "xyzpdq" + random.nextDouble(), random.nextBoolean(), metaData);
    }

    @Test
    public void testTasks()
    {
        List<TaskModel> tasks = Lists.newArrayList();
        int qty = random.nextInt(100);
        for ( int i = 0; i < qty; ++i )
        {
            tasks.add(makeTask());
        }
        JsonNode node = newTasks(tasks);
        String str = nodeToString(node);
        System.out.println(str);

        List<TaskModel> unTasks = getTasks(fromString(str));
        Assert.assertEquals(tasks, unTasks);
    }

    @Test
    public void testTaskSet()
    {
        TaskSets taskSets = makeTaskSet();

        JsonNode node = newTaskSet(taskSets);
        String str = nodeToString(node);
        System.out.println(str);

        TaskSets unTaskSets = getTaskSet(fromString(str));
        Assert.assertEquals(taskSets, unTaskSets);
    }

    @Test
    public void testSchedule()
    {
        ScheduleModel schedule = makeSchedule();

        JsonNode node = newSchedule(schedule);
        String str = nodeToString(node);
        System.out.println(str);

        ScheduleModel unSchedule = getSchedule(fromString(str));
        Assert.assertEquals(schedule, unSchedule);
    }

    @Test
    public void testSchedules()
    {
        List<ScheduleModel> schedules = Lists.newArrayList();
        for ( int i = 0; i < (random.nextInt(9) + 1); ++i )
        {
            schedules.add(makeSchedule());
        }

        JsonNode node = newSchedules(schedules);
        String str = nodeToString(node);
        System.out.println(str);

        List<ScheduleModel> unSchedules = getSchedules(fromString(str));
        Assert.assertEquals(schedules, unSchedules);
    }

    @Test
    public void testWorkflow()
    {
        WorkflowModel workflow = new WorkflowModel(new WorkflowId(), "iqlrhawlksFN", makeTaskSet());

        JsonNode node = newWorkflow(workflow);
        String str = nodeToString(node);
        System.out.println(str);

        WorkflowModel unWorkflow = getWorkflow(fromString(str));
        Assert.assertEquals(workflow, unWorkflow);
    }

    @Test
    public void testWorkflows()
    {
        List<WorkflowModel> workflows = Lists.newArrayList();
        for ( int i = 0; i < random.nextInt(10); ++i )
        {
            WorkflowModel workflow = new WorkflowModel(new WorkflowId(), Integer.toString(random.nextInt()), makeTaskSet());
            workflows.add(workflow);
        }

        JsonNode node = newWorkflows(workflows);
        String str = nodeToString(node);
        System.out.println(str);

        List<WorkflowModel> unWorkflows = getWorkflows(fromString(str));
        Assert.assertEquals(workflows, unWorkflows);
    }

    @Test
    public void testScheduleExecution()
    {
        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(new ScheduleId(), LocalDateTime.now(), LocalDateTime.now(), random.nextInt());

        JsonNode node = newScheduleExecution(scheduleExecution);
        String str = nodeToString(node);
        System.out.println(str);

        ScheduleExecutionModel unScheduleExecution = getScheduleExecution(fromString(str));
        Assert.assertEquals(scheduleExecution, unScheduleExecution);
    }

    @Test
    public void testDenormalizedWorkflow() throws Exception
    {
        WorkflowModel workflow = new WorkflowModel(new WorkflowId(), "iqlrhawlksFN", makeTaskSet());
        List<TaskModel> tasks = Lists.newArrayList();

        for ( List<TaskId> taskSet : workflow.getTasks() )
        {
            for ( TaskId taskId : taskSet )
            {
                tasks.add(makeTask(taskId));
            }
        }

        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(new ScheduleId(), LocalDateTime.now(), LocalDateTime.now(), random.nextInt());
        DenormalizedWorkflowModel denormalizedWorkflowModel = new DenormalizedWorkflowModel(new RunId(), scheduleExecution, workflow.getWorkflowId(), tasks, workflow.getName(), workflow.getTasks(), LocalDateTime.now(), random.nextInt());

        ObjectNode node = newDenormalizedWorkflow(denormalizedWorkflowModel);
        String str = nodeToString(node);
        System.out.println(str);

        DenormalizedWorkflowModel unDenormalizedWorkflow = getDenormalizedWorkflow(fromString(str));
        Assert.assertEquals(denormalizedWorkflowModel, unDenormalizedWorkflow);
    }

    @Test
    public void testExecutableTask()
    {
        ExecutableTaskModel executableTask = new ExecutableTaskModel(new RunId(), new ScheduleId(), makeTask());

        JsonNode node = newExecutableTask(executableTask);
        String str = nodeToString(node);
        System.out.println(str);

        ExecutableTaskModel unExecutableTask = getExecutableTask(fromString(str));
        Assert.assertEquals(executableTask, unExecutableTask);
    }

    @Test
    public void testTaskExecutionResult()
    {
        Map<String, String> resultData = Maps.newHashMap();
        resultData.put("one", "1");
        resultData.put("two", "2");
        TaskExecutionResult taskExecutionResult = new TaskExecutionResult(Integer.toString(random.nextInt()), resultData);
        JsonNode node = newTaskExecutionResult(taskExecutionResult);
        String str = nodeToString(node);
        System.out.println(str);

        TaskExecutionResult unTaskExecutionResult = getTaskExecutionResult(fromString(str));
        Assert.assertEquals(taskExecutionResult, unTaskExecutionResult);
    }

    @Test
    public void testStartedTask()
    {
        StartedTaskModel startedTask = new StartedTaskModel("test", LocalDateTime.now());
        JsonNode node = newStartedTask(startedTask);
        String str = nodeToString(node);
        System.out.println(str);

        StartedTaskModel unStartedTask = getStartedTask(fromString(str));
        Assert.assertEquals(startedTask, unStartedTask);
    }

    @Test
    public void testTaskDag()
    {
        TaskDagModel taskDag = makeTaskDag(random.nextInt(3) + 1);
        JsonNode node = newTaskDag(taskDag);
        String str = nodeToString(node);
        System.out.println(str);

        TaskDagModel unTaskDag = getTaskDag(fromString(str));
        Assert.assertEquals(taskDag, unTaskDag);
    }

    @Test
    public void testFileTaskDag() throws Exception
    {
        String json = Resources.toString(Resources.getResource("big_task_dag.json"), Charset.defaultCharset());
        TaskDagModel taskDag = getTaskDag(fromString(json));
        JsonNode node = newTaskDag(taskDag);
        String str = nodeToString(node);
        System.out.println(str);

        TaskDagModel unTaskDag = getTaskDag(fromString(str));
        Assert.assertEquals(taskDag, unTaskDag);
    }

    private TaskDagModel makeTaskDag(int remaining)
    {
        List<TaskId> tasks = Lists.newArrayList();
        List<TaskDagModel> siblings = Lists.newArrayList();
        List<TaskDagModel> children = Lists.newArrayList();

        for ( int i = 0; i < (random.nextInt(9) + 1); ++i )
        {
            tasks.add(new TaskId());
        }
        if ( remaining > 0 )
        {
            for ( int i = 0; i < (random.nextInt(3) + 1); ++i )
            {
                siblings.add(makeTaskDag(remaining - 1));
            }
            for ( int i = 0; i < (random.nextInt(3) + 1); ++i )
            {
                children.add(makeTaskDag(remaining - 1));
            }
        }

        return new TaskDagModel(tasks, siblings, children);
    }

    private ScheduleModel makeSchedule()
    {
        RepetitionModel.Type type = random.nextBoolean() ? RepetitionModel.Type.ABSOLUTE : RepetitionModel.Type.RELATIVE;
        return new ScheduleModel(new ScheduleId(), new WorkflowId(), new RepetitionModel(new Duration(Math.abs(random.nextInt()), TimeUnit.MINUTES), type, random.nextInt()));
    }

    private TaskSets makeTaskSet()
    {
        List<TaskId> ids1 = Arrays.asList(new TaskId(), new TaskId(), new TaskId(), new TaskId(), new TaskId(), new TaskId());
        List<TaskId> ids2 = Arrays.asList(new TaskId(), new TaskId(), new TaskId(), new TaskId());
        List<TaskId> ids3 = Arrays.asList(new TaskId());
        List<List<TaskId>> tasks = ImmutableList.of(ids1, ids2, ids3);
        return new TaskSets(tasks);
    }
}
