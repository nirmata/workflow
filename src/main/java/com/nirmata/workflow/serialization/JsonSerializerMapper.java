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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Map;

public class JsonSerializerMapper
{
    private final Map<Class<?>, Methods> methodsMap;

    private static final String NEW_PREFIX = "new";
    private static final String GET_PREFIX = "get";

    private static final String MODELS_PACKAGE = "com.nirmata.workflow.models.";
    private static final String INTERNAL_MODELS_PACKAGE = "com.nirmata.workflow.details.internalmodels.";

    private static class Methods
    {
        private final Method getter;
        private final Method newer;

        public Methods(Method getter, Method newer)
        {
            this.getter = getter;
            this.newer = newer;
        }
    }

    public JsonSerializerMapper()
    {
        ImmutableMap.Builder<Class<?>, Methods> builder = ImmutableMap.builder();
        for ( Method method : JsonSerializer.class.getDeclaredMethods() )
        {
            if ( method.getName().startsWith(NEW_PREFIX) && (method.getReturnType() == JsonNode.class) && (method.getParameterTypes().length == 1) )
            {
                String className = method.getName().substring(NEW_PREFIX.length());
                Method getter = getGetter(className);
                if ( getter != null )
                {
                    Class<?> clazz = getClazz(className);
                    if ( (clazz != null) && (method.getParameterTypes()[0] == clazz) )
                    {
                        builder.put(clazz, new Methods(getter, method));
                    }
                }
            }
        }
        methodsMap = builder.build();
    }

    public <T> JsonNode make(T obj)
    {
        Methods methods = methodsMap.get(obj.getClass());
        methods = Preconditions.checkNotNull(methods, "No serializer found for: " + obj.getClass());
        try
        {
            return (JsonNode)methods.newer.invoke(null, obj);
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not invoke newer() for: " + obj.getClass(), e);
        }
    }

    public ObjectMapper getMapper()
    {
        return JsonSerializer.getMapper();
    }

    public <T> T get(JsonNode node, Class<T> clazz)
    {
        Methods methods = methodsMap.get(clazz);
        methods = Preconditions.checkNotNull(methods, "No serializer found for: " + clazz);
        try
        {
            return clazz.cast(methods.getter.invoke(null, node));
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not invoke getter() for: " + clazz, e);
        }
    }

    private Method getGetter(String className)
    {
        try
        {
            return JsonSerializer.class.getDeclaredMethod(GET_PREFIX + className, JsonNode.class);
        }
        catch ( NoSuchMethodException ignore )
        {
            // ignore
        }
        return null;
    }

    private Class<?> getClazz(String className)
    {
        Class<?> clazz = null;
        try
        {
            clazz = Class.forName(MODELS_PACKAGE + className);
        }
        catch ( ClassNotFoundException ignore )
        {
            try
            {
                clazz = Class.forName(INTERNAL_MODELS_PACKAGE + className);
            }
            catch ( ClassNotFoundException ignore2 )
            {
                // ignore
            }
        }
        return clazz;
    }
}
