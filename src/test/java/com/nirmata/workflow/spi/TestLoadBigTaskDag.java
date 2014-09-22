package com.nirmata.workflow.spi;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.nirmata.workflow.models.DagId;
import com.nirmata.workflow.models.TaskDagContainerModel;
import com.nirmata.workflow.models.TaskDagModel;
import com.nirmata.workflow.models.TaskId;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static com.nirmata.workflow.spi.JsonSerializer.*;

public class TestLoadBigTaskDag
{
    @Test
    public void loadBigTaskDag() throws Exception
    {
        TaskDagModel dag1 = buildDag1();
        TaskDagModel dag2 = new TaskDagModel(new TaskId("2-1"), Lists.newArrayList());
        TaskDagContainerModel model1 = new TaskDagContainerModel(new DagId("dag-1"), dag1);
        TaskDagContainerModel model2 = new TaskDagContainerModel(new DagId("dag-2"), dag2);
        List<TaskDagContainerModel> expectedContainers = Arrays.asList(model1, model2);

        String json = Resources.toString(Resources.getResource("big_task_dag.json"), Charset.defaultCharset());
        List<TaskDagContainerModel> loadedContainers = getTaskDagContainers(fromString(json));
        System.out.println(loadedContainers);

        Assert.assertEquals(expectedContainers, loadedContainers);

        List<TaskDagContainerModel> reloadedContainers = getTaskDagContainers(fromString(nodeToString(newTaskDagContainers(expectedContainers))));
        Assert.assertEquals(reloadedContainers, loadedContainers);
    }

    private TaskDagModel buildDag1()
    {
        TaskDagModel task13 = new TaskDagModel(new TaskId("task-13"), Lists.newArrayList());
        TaskDagModel task12 = new TaskDagModel(new TaskId("task-12"), Lists.newArrayList(task13));
        TaskDagModel task11 = new TaskDagModel(new TaskId("task-11"), Lists.newArrayList(task13));
        TaskDagModel task10 = new TaskDagModel(new TaskId("task-10"), Lists.newArrayList(task13));
        TaskDagModel task9 = new TaskDagModel(new TaskId("task-9"), Lists.newArrayList(task12));
        TaskDagModel task8 = new TaskDagModel(new TaskId("task-8"), Lists.newArrayList(task11));
        TaskDagModel task7 = new TaskDagModel(new TaskId("task-7"), Lists.newArrayList(task10));
        TaskDagModel task6 = new TaskDagModel(new TaskId("task-6"), Lists.newArrayList(task7, task8, task9));
        TaskDagModel task5 = new TaskDagModel(new TaskId("task-5"), Lists.newArrayList(task13));
        TaskDagModel task4 = new TaskDagModel(new TaskId("task-4"), Lists.newArrayList(task13));
        TaskDagModel task3 = new TaskDagModel(new TaskId("task-3"), Lists.newArrayList(task4));
        TaskDagModel task2 = new TaskDagModel(new TaskId("task-2"), Lists.newArrayList(task4));
        TaskDagModel task1 = new TaskDagModel(new TaskId("task-1"), Lists.newArrayList(task2, task3));
        return new TaskDagModel(new TaskId(""), Arrays.asList(task1, task5, task6));
    }
}
