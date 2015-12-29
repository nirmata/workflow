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

import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

public class TaskQueueSerializer implements QueueSerializer<ExecutableTask>
{
    private final Serializer serializer;

    public TaskQueueSerializer(Serializer serializer)
    {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(ExecutableTask executableTask)
    {
        return serializer.serialize(executableTask);
    }

    @Override
    public ExecutableTask deserialize(byte[] bytes)
    {
        return serializer.deserialize(bytes, ExecutableTask.class);
    }
}
