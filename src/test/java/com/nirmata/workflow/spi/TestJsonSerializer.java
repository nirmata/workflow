package com.nirmata.workflow.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.details.Clock;
import com.nirmata.workflow.details.StateCache;
import com.nirmata.workflow.details.internalmodels.CompletedTaskModel;
import com.nirmata.workflow.details.internalmodels.DenormalizedWorkflowModel;
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
        String str = JsonSerializer.toString(node);
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
        String str = JsonSerializer.toString(node);
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
        String str = JsonSerializer.toString(node);
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
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        TaskSets unTaskSets = getTaskSet(fromString(str));
        Assert.assertEquals(taskSets, unTaskSets);
    }

    @Test
    public void testSchedule()
    {
        ScheduleModel schedule = new ScheduleModel(new ScheduleId(), new WorkflowId(), new RepetitionModel(new Duration(10064, TimeUnit.MINUTES), RepetitionModel.Type.ABSOLUTE, random.nextInt()));

        ObjectNode node = newNode();
        addSchedule(node, schedule);
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        ScheduleModel unSchedule = getSchedule(fromString(str));
        Assert.assertEquals(schedule, unSchedule);
    }

    @Test
    public void testWorkflow()
    {
        WorkflowModel workflow = new WorkflowModel(new WorkflowId(), "iqlrhawlksFN", makeTaskSet());

        ObjectNode node = newNode();
        addWorkflow(node, workflow);
        String str = JsonSerializer.toString(node);
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
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        ScheduleExecutionModel unScheduleExecution = getScheduleExecution(fromString(str));
        Assert.assertEquals(scheduleExecution, unScheduleExecution);
    }

    @Test
    public void testDenormalizedWorkflow()
    {
        WorkflowModel workflow = new WorkflowModel(new WorkflowId(), "iqlrhawlksFN", makeTaskSet());
        List<ScheduleModel> schedules = Lists.newArrayList();
        List<ScheduleExecutionModel> scheduleExecutions = Lists.newArrayList();
        List<TaskModel> tasks = Lists.newArrayList();
        List<WorkflowModel> workflows = Lists.newArrayList(workflow);

        for ( List<TaskId> taskSet : workflow.getTasks() )
        {
            for ( TaskId taskId : taskSet )
            {
                tasks.add(makeTask(taskId));
            }
        }

        StateCache cache = new StateCache(schedules, scheduleExecutions, tasks, workflows);

        Date nowUtc = Clock.nowUtc();
        ObjectNode node = newNode();
        addDenormalizedWorkflow(node, cache, workflow.getWorkflowId(), nowUtc);
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        DenormalizedWorkflowModel denormalizedWorkflowModel = new DenormalizedWorkflowModel(workflow.getWorkflowId(), tasks, workflow.getName(), workflow.getTasks(), nowUtc);
        DenormalizedWorkflowModel unDenormalizedWorkflow = getDenormalizedWorkflow(fromString(str));
        Assert.assertEquals(denormalizedWorkflowModel, unDenormalizedWorkflow);
    }

    @Test
    public void testCompletedTask()
    {
        CompletedTaskModel completedTask = new CompletedTaskModel();
        ObjectNode node = newNode();
        addCompletedTask(node, completedTask);
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        CompletedTaskModel unCompletedTask = getCompletedTask(fromString(str));
        Assert.assertEquals(completedTask, unCompletedTask);

        Map<String, String> resultData = Maps.newHashMap();
        resultData.put("one", "1");
        resultData.put("two", "2");
        completedTask = new CompletedTaskModel(true, resultData);
        node = newNode();
        addCompletedTask(node, completedTask);
        str = JsonSerializer.toString(node);
        System.out.println(str);

        unCompletedTask = getCompletedTask(fromString(str));
        Assert.assertEquals(completedTask, unCompletedTask);
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
