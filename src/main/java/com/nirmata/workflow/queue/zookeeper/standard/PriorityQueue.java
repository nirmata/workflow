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
package com.nirmata.workflow.queue.zookeeper.standard;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.models.ExecutableTask;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import java.io.IOException;

class PriorityQueue implements InternalQueueBase
{
    private final DistributedPriorityQueue<ExecutableTask> queue;

    PriorityQueue(DistributedPriorityQueue<ExecutableTask> queue)
    {
        this.queue = queue;
    }

    @Override
    public void start() throws Exception
    {
        queue.start();
    }

    @Override
    public void put(ExecutableTask item, long value) throws Exception
    {
        Preconditions.checkArgument(value <= Integer.MAX_VALUE, "priority is too large: " + value);
        queue.put(item, (int)value);
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }
}
