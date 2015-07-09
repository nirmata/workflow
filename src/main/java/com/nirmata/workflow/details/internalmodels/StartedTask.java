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
package com.nirmata.workflow.details.internalmodels;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.time.LocalDateTime;

public class StartedTask implements Serializable
{
    private final String instanceName;
    private final LocalDateTime startDateUtc;
    private int progress;

    public StartedTask(String instanceName, LocalDateTime startDateUtc)
    {
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        this.startDateUtc = Preconditions.checkNotNull(startDateUtc, "startDateUtc cannot be null");
        this.progress = 0;
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    public LocalDateTime getStartDateUtc()
    {
        return startDateUtc;
    }
    
    public int getProgress()
    {
        return progress;
    }
    
    public void setProgress(int progress)
    {
        this.progress = progress;
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

        StartedTask that = (StartedTask)o;

        if ( !instanceName.equals(that.instanceName) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( !startDateUtc.equals(that.startDateUtc) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = instanceName.hashCode();
        result = 31 * result + startDateUtc.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "StartedTaskModel{" +
            "instanceName='" + instanceName + '\'' +
            ", startDateUtc=" + startDateUtc +
            '}';
    }
}
