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
