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

import com.google.common.collect.Lists;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.StandardAutoCleaner;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.details.AutoCleanerHolder;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestAutoCleanerHolder
{
    @Test
    public void testNull() throws InterruptedException
    {
        Timing timing = new Timing();
        AutoCleanerHolder holder = new AutoCleanerHolder(null, Duration.ofDays(10));
        Assert.assertFalse(holder.shouldRun());
        Assert.assertFalse(holder.shouldRun());
        timing.sleepABit();
        Assert.assertFalse(holder.shouldRun());
        Assert.assertFalse(holder.shouldRun());

        Assert.assertNotNull(holder.getRunPeriod());
    }

    @Test
    public void testPeriod() throws InterruptedException
    {
        AutoCleanerHolder holder = new AutoCleanerHolder(new StandardAutoCleaner(Duration.ofSeconds(2)), Duration.ofSeconds(1));
        Assert.assertFalse(holder.shouldRun());
        TimeUnit.SECONDS.sleep(2);
        Assert.assertTrue(holder.shouldRun());

        RunId runningId = new RunId();
        RunId completedId = new RunId();
        RunId completedButRecentId = new RunId();
        List<RunInfo> runs = Lists.newArrayList(
            new RunInfo(runningId, LocalDateTime.now(Clock.systemUTC())),
            new RunInfo(completedButRecentId, LocalDateTime.now(Clock.systemUTC()), LocalDateTime.now(Clock.systemUTC())),
            new RunInfo(completedId, LocalDateTime.now(Clock.systemUTC()), LocalDateTime.now(Clock.systemUTC()).minusSeconds(3))
        );

        List<RunId> cleaned = Lists.newArrayList();
        WorkflowAdmin admin = new WorkflowAdmin()
        {
            @Override
            public List<RunInfo> getRunInfo()
            {
                return runs;
            }

            @Override
            public RunInfo getRunInfo(RunId runId)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<TaskInfo> getTaskInfo(RunId runId)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean clean(RunId runId)
            {
                cleaned.add(runId);
                return true;
            }

            @Override
            public Map<TaskId, TaskDetails> getTaskDetails(RunId runId)
            {
                throw new UnsupportedOperationException();
            }
        };

        holder.run(admin);
        Assert.assertEquals(cleaned.size(), 1);
        Assert.assertEquals(cleaned.get(0), completedId);
    }
}
