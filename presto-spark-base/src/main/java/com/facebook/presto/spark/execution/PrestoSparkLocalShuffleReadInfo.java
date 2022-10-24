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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;

public class PrestoSparkLocalShuffleReadInfo
        implements PrestoSparkShuffleReadInfo<PrestoSparkLocalShuffleReadInfo>
{
    private static final JsonCodec<PrestoSparkLocalShuffleReadInfo> SHUFFLE_READ_INFO_CODEC = new JsonCodecFactory().jsonCodec(PrestoSparkLocalShuffleReadInfo.class);
    private final int numberOfPartitions;
    private final String rootPath;
    private final long maxBytesPerPartition;

    public PrestoSparkLocalShuffleReadInfo(int numberOfPartitions, String rootPath, long maxBytesPerPartition)
    {
        this.numberOfPartitions = numberOfPartitions;
        this.rootPath = rootPath;
        this.maxBytesPerPartition = maxBytesPerPartition;
    }

    @Override
    public String toJson()
    {
        return SHUFFLE_READ_INFO_CODEC.toJson(this);
    }
}