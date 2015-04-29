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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;

public class StandardSerializer implements Serializer
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final JsonSerializerMapper mapper = new JsonSerializerMapper();

    @Override
    public <T> byte[] serialize(T obj)
    {
        try
        {
            JsonNode node = mapper.make(obj);
            return JsonSerializer.getMapper().writeValueAsBytes(node);
        }
        catch ( JsonProcessingException e )
        {
            log.error("Could not serialize: " + obj.getClass(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz)
    {
        try
        {
            JsonNode node = JsonSerializer.getMapper().readTree(data);
            return mapper.get(node, clazz);
        }
        catch ( IOException e )
        {
            log.error("Could not deserialize: " + Arrays.toString(data), e);
            throw new RuntimeException(e);
        }
    }
}
