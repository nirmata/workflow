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
package com.nirmata.workflow.events;

import org.apache.curator.framework.listen.Listenable;
import java.io.Closeable;

/**
 * A listener manager for events. Once {@link #start()} is called, workflow
 * events will be generated as runs and tasks are started, completed, etc.
 * When through, call {@link #close()}.
 */
public interface WorkflowListenerManager extends Closeable
{
    /**
     * Start listening for events. NOTE: previous events are not reported.
     * i.e. already started runs/tasks are not reported.
     */
    void start();

    /**
     * Return the container to add/remove event listeners
     *
     * @return container
     */
    Listenable<WorkflowListener> getListenable();
}
