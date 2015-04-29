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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JDKSerializer implements Serializer
{
    @Override
    public <T> byte[] serialize(T obj)
    {
        ByteArrayOutputStream bytes = null;
        try
        {
            bytes = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bytes);
            out.writeObject(obj);
            out.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
        return bytes.toByteArray();
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz)
    {
        try
        {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
            return clazz.cast(in.readObject());
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }
}
