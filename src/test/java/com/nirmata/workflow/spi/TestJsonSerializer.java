package com.nirmata.workflow.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.models.*;
import io.airlift.units.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.nirmata.workflow.details.InternalJsonSerializer.*;
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

        ObjectNode node = newNode();
        addTask(node, task);
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
        ObjectNode node = newNode();
        addTasks(node, tasks);
        String str = nodeToString(node);
        System.out.println(str);

        List<TaskModel> unTasks = getTasks(fromString(str));
        Assert.assertEquals(tasks, unTasks);
    }

    @Test
    public void testTaskSet()
    {
        TaskSets taskSets = makeTaskSet();

        ObjectNode node = newNode();
        addTaskSet(node, taskSets);
        String str = nodeToString(node);
        System.out.println(str);

        TaskSets unTaskSets = getTaskSet(fromString(str));
        Assert.assertEquals(taskSets, unTaskSets);
    }

    @Test
    public void testSchedule()
    {
        ScheduleModel schedule = newSchedule();

        ObjectNode node = newNode();
        addSchedule(node, schedule);
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
            schedules.add(newSchedule());
        }

        ObjectNode node = newNode();
        addSchedules(node, schedules);
        String str = nodeToString(node);
        System.out.println(str);

        List<ScheduleModel> unSchedules = getSchedules(fromString(str));
        Assert.assertEquals(schedules, unSchedules);
    }

    @Test
    public void testWorkflow()
    {
        WorkflowModel workflow = new WorkflowModel(new WorkflowId(), "iqlrhawlksFN", makeTaskSet());

        ObjectNode node = newNode();
        addWorkflow(node, workflow);
        String str = nodeToString(node);
        System.out.println(str);

        WorkflowModel unWorkflow = getWorkflow(fromString(str));
        Assert.assertEquals(workflow, unWorkflow);
    }

    @Test
    public void testScheduleExecution()
    {
        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(new ScheduleId(), new Date(), new Date(), random.nextInt());

        ObjectNode node = newNode();
        addScheduleExecution(node, scheduleExecution);
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

        Date randomDate = Clock.nowUtc();
        Thread.sleep(5);
        Date randomDate2 = Clock.nowUtc();
        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(new ScheduleId(), randomDate, randomDate2, random.nextInt());
        DenormalizedWorkflowModel denormalizedWorkflowModel = new DenormalizedWorkflowModel(scheduleExecution, workflow.getWorkflowId(), tasks, workflow.getName(), workflow.getTasks(), Clock.nowUtc(), random.nextInt());

        ObjectNode node = newNode();
        addDenormalizedWorkflow(node, denormalizedWorkflowModel);
        String str = nodeToString(node);
        System.out.println(str);

        DenormalizedWorkflowModel unDenormalizedWorkflow = getDenormalizedWorkflow(fromString(str));
        Assert.assertEquals(denormalizedWorkflowModel, unDenormalizedWorkflow);
    }

    @Test
    public void testExecutableTask()
    {
        ExecutableTaskModel executableTask = new ExecutableTaskModel(new ScheduleId(), makeTask());

        ObjectNode node = newNode();
        addExecutableTask(node, executableTask);
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
        ObjectNode node = newNode();
        addTaskExecutionResult(node, taskExecutionResult);
        String str = nodeToString(node);
        System.out.println(str);

        TaskExecutionResult unTaskExecutionResult = getTaskExecutionResult(fromString(str));
        Assert.assertEquals(taskExecutionResult, unTaskExecutionResult);
    }

    private ScheduleModel newSchedule()
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
