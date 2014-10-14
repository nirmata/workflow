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
package com.nirmata.workflow.executor;

/**
 * Task execution status
 */
public enum TaskExecutionStatus
{
    /**
     * The task executed successfully
     */
    SUCCESS()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

    /**
     * The task failed, but the remaining tasks should still execute
     */
    FAILED_CONTINUE()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return false;
        }
    },

    /**
     * The task failed and the remaining tasks in the run should be canceled
     */
    FAILED_STOP()
    {
        @Override
        public boolean isCancelingStatus()
        {
            return true;
        }
    }
    ;

    public abstract boolean isCancelingStatus();
}
