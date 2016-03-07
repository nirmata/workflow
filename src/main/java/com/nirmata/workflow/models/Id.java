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
package com.nirmata.workflow.models;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.UUID;

/**
 * Base for IDs
 */
public class Id implements Serializable
{
    private final String id;

    protected Id()
    {
        id = newRandomId();
    }

    static String newRandomId()
    {
        return UUID.randomUUID().toString();
    }

    protected Id(String id)
    {
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public boolean isValid()
    {
        return (id.length() > 0);
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Id id1 = (Id)o;

        //noinspection RedundantIfStatement
        if ( !id.equals(id1.id) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return "Id{" +
            "id='" + id + '\'' +
            '}';
    }
}
