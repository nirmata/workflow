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

public class RunId extends Id
{
    /**
     * Generate a new, globally unique ID that has the given prefix.
     * E.g. <code>newRandomIdWithPrefix("test")</code> generates: <code>test-828119e0-cd47-45a1-b120-94d284ecb7b3</code>
     *
     * @param prefix ID's prefix
     * @return new ID
     */
    public static RunId newRandomIdWithPrefix(String prefix)
    {
        return new RunId(prefix + "-" + newRandomId());
    }

    public static void main(String[] args)
    {
        System.out.println(newRandomIdWithPrefix("hey"));
        System.out.println(newRandomIdWithPrefix("hey"));
        System.out.println(newRandomIdWithPrefix("hey"));
        System.out.println(newRandomIdWithPrefix("hey"));
    }

    public RunId()
    {
    }

    public RunId(String id)
    {
        super(id);
    }
}
