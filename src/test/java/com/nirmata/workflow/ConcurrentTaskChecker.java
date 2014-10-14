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
import com.google.common.collect.Sets;
import com.nirmata.workflow.models.TaskId;
import org.testng.Assert;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class ConcurrentTaskChecker
{
    private final Set<TaskId> currentSet = Sets.newHashSet();
    private final List<TaskId> all = Lists.newArrayList();
    private int count = 0;
    private final List<Set<TaskId>> sets = Lists.newArrayList();

    synchronized void reset()
    {
        currentSet.clear();
        all.clear();
        sets.clear();
        count = 0;
    }

    synchronized void add(TaskId taskId)
    {
        all.add(taskId);
        currentSet.add(taskId);
        ++count;
    }

    synchronized void decrement()
    {
        if ( --count == 0 )
        {
            HashSet<TaskId> copy = Sets.newHashSet(currentSet);
            currentSet.clear();
            count = 0;
            sets.add(copy);
        }
    }

    synchronized List<Set<TaskId>> getSets()
    {
        return Lists.newArrayList(sets);
    }

    synchronized List<TaskId> getAll()
    {
        return Lists.newArrayList(all);
    }

    synchronized void assertNoDuplicates()
    {
        Assert.assertEquals(all.size(), Sets.newHashSet(all).size());   // no dups
    }
}
