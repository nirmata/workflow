package com.nirmata.workflow.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
        Map<String, String> metaData = Maps.newHashMap();
        if ( random.nextBoolean() )
        {
            int qty = random.nextInt(25);
            for ( int i = 0; i < qty; ++i  )
            {
                metaData.put(Integer.toString(i), "" + random.nextInt());
            }
        }
        return new TaskModel(new TaskId(), "test" + random.nextDouble(), "xyzpdq" + random.nextDouble(), metaData);
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
        TaskSet taskSet = makeTaskSet();

        ObjectNode node = newNode();
        addTaskSet(node, taskSet);
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        TaskSet unTaskSet = getTaskSet(fromString(str));
        Assert.assertEquals(taskSet, unTaskSet);
    }

    @Test
    public void testSchedule()
    {
        ScheduleModel schedule = new ScheduleModel(new ScheduleId(), new WorkflowId(), new Repetition(new Duration(10064, TimeUnit.MINUTES), Repetition.Type.ABSOLUTE, random.nextInt()));

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
        ScheduleExecutionModel scheduleExecution = new ScheduleExecutionModel(new ScheduleId(), new Date(), random.nextInt());

        ObjectNode node = newNode();
        addScheduleExecution(node, scheduleExecution);
        String str = JsonSerializer.toString(node);
        System.out.println(str);

        ScheduleExecutionModel unScheduleExecution = getScheduleExecution(fromString(str));
        Assert.assertEquals(scheduleExecution, unScheduleExecution);
    }

    private TaskSet makeTaskSet()
    {
        List<TaskId> ids = Arrays.asList(new TaskId(), new TaskId(), new TaskId(), new TaskId(), new TaskId(), new TaskId());
        return new TaskSet(ids);
    }
}
