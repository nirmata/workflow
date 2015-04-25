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
package com.nirmata.workflow;

import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.models.Task;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.internal.Maps;
import org.assertj.core.internal.Objects;

import static org.assertj.core.util.Arrays.array;

public class WorkflowAssertions extends Assertions
{
    static final Objects objects = Objects.instance();
    static final Maps maps = Maps.instance();

    public static RunInfoAssert assertThat(RunInfo actual)
    {
        return new RunInfoAssert(actual);
    }

    public static final class RunInfoAssert extends AbstractAssert<RunInfoAssert, RunInfo>
    {

        RunInfoAssert(RunInfo actual)
        {
            super(actual, RunInfoAssert.class);
        }

        public RunInfoAssert isComplete()
        {
            isNotNull();
            if (!actual.isComplete()) {
                failWithMessage("\nExpecting <%s> to be complete", actual);
            }
            return myself;
        }

        public RunInfoAssert isNotComplete()
        {
            isNotNull();
            if (actual.isComplete()) {
                failWithMessage("\nExpecting <%s> to not be complete", actual);
            }
            return myself;
        }
    }

    public static TaskDetailsAssert assertThat(TaskDetails actual)
    {
        return new TaskDetailsAssert(actual);
    }

    public static final class TaskDetailsAssert extends AbstractAssert<TaskDetailsAssert, TaskDetails>
    {
        TaskDetailsAssert(TaskDetails actual)
        {
            super(actual, TaskDetailsAssert.class);
        }

        public TaskDetailsAssert matchesTask(Task expected)
        {
            isNotNull();
            objects.assertEqual(info, actual.getTaskId(), expected.getTaskId());
            if (expected.isExecutable()) {
                objects.assertEqual(info, actual.getTaskType(), expected.getTaskType());
            }
            objects.assertEqual(info, actual.getMetaData(), expected.getMetaData());
            return this;
        }
    }

    public static TaskInfoAssert assertThat(TaskInfo actual)
    {
        return new TaskInfoAssert(actual);
    }

    public static final class TaskInfoAssert extends AbstractAssert<TaskInfoAssert, TaskInfo>
    {

        TaskInfoAssert(TaskInfo actual)
        {
            super(actual, TaskInfoAssert.class);
        }

        public TaskInfoAssert hasNotStarted()
        {
            isNotNull();
            if (actual.hasStarted()) {
                failWithMessage("\nExpecting <%s> to not have started", actual);
            }
            return myself;
        }

        public TaskInfoAssert isComplete()
        {
            isNotNull();
            if (!actual.hasStarted() || !actual.isComplete()) {
                failWithMessage("\nExpecting <%s> to be complete", actual);
            }
            maps.assertContains(info, actual.getResult().getResultData(), array(entry("taskId", actual.getTaskId().getId())));
            return myself;
        }

        public TaskInfoAssert isNotComplete()
        {
            isNotNull();
            if (!actual.hasStarted()) {
                failWithMessage("\nExpecting <%s> to have started", actual);
            }
            if (actual.isComplete()) {
                failWithMessage("\nExpecting <%s> to not be complete", actual);
            }
            return myself;
        }
    }
}
