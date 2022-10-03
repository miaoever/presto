/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.classloader_interface;

public class PrestoSparkShuffleTaskSource
{
    private final String planNodeId;
    private final PrestoSparkShuffleDescriptor shuffleDescriptor;

    public PrestoSparkShuffleTaskSource(String planNodeId, PrestoSparkShuffleDescriptor shuffleDescriptor)
    {
        this.planNodeId = planNodeId;
        this.shuffleDescriptor = shuffleDescriptor;
    }

    public String getPlanNodeId()
    {
        return planNodeId;
    }

    public PrestoSparkShuffleDescriptor getShuffleDescriptor()
    {
        return shuffleDescriptor;
    }
}
