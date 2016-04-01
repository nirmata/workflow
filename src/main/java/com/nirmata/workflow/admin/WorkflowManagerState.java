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
package com.nirmata.workflow.admin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class WorkflowManagerState
{
    private final boolean isConnectedToZooKeeper;
    private final State schedulerState;
    private final List<State> consumersState;

    public enum State
    {
        /**
         * Has not started
         */
        LATENT,

        /**
         * Is in a blocking method
         */
        SLEEPING,

        /**
         * Is doing work
         */
        PROCESSING,

        /**
         * Has been closed
         */
        CLOSED
    }

    public WorkflowManagerState(boolean isConnectedToZooKeeper, State schedulerState, List<State> consumersState)
    {
        this.isConnectedToZooKeeper = isConnectedToZooKeeper;
        this.schedulerState = Preconditions.checkNotNull(schedulerState, "schedulerState cannot be null");
        this.consumersState = ImmutableList.copyOf(Preconditions.checkNotNull(consumersState, "consumersState cannot be null"));
    }

    /**
     * Returns true if the connection to ZooKeeper is active
     *
     * @return true/false
     */
    public boolean isConnectedToZooKeeper()
    {
        return isConnectedToZooKeeper;
    }

    /**
     * Return the state of the scheduler for this manager instance. If {@link State#LATENT} is returned
     * this instance is not currently the scheduler.
     *
     * @return state info
     */
    public State getSchedulerState()
    {
        return schedulerState;
    }

    /**
     * Return state info for each task executor in this manager
     *
     * @return task executors state
     */
    public List<State> getExecutorsState()
    {
        return consumersState;
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

        WorkflowManagerState that = (WorkflowManagerState)o;

        if ( isConnectedToZooKeeper != that.isConnectedToZooKeeper )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( schedulerState != that.schedulerState )
        {
            return false;
        }
        return consumersState.equals(that.consumersState);

    }

    @Override
    public int hashCode()
    {
        int result = (isConnectedToZooKeeper ? 1 : 0);
        result = 31 * result + schedulerState.hashCode();
        result = 31 * result + consumersState.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "WorkflowManagerState{" +
            "isConnectedToZooKeeper=" + isConnectedToZooKeeper +
            ", schedulerState=" + schedulerState +
            ", consumersState=" + consumersState +
            '}';
    }
}
