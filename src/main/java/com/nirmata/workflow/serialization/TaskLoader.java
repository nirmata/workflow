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
package com.nirmata.workflow.serialization;

import com.nirmata.workflow.models.Task;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Utility for loading Tasks from a file/stream
 */
public class TaskLoader
{
    /**
     * Load a complete task with children from a JSON stream
     *
     * @param jsonStream the JSON stream
     * @return task
     */
    public static Task load(Reader jsonStream)
    {
        try
        {
            return JsonSerializer.getTask(JsonSerializer.getMapper().readTree(jsonStream));
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load a complete task with children from a JSON string
     *
     * @param json the JSON
     * @return task
     */
    public static Task load(String json)
    {
        try ( StringReader reader = new StringReader(json) )
        {
            return load(reader);
        }
    }

    private TaskLoader()
    {
    }
}
