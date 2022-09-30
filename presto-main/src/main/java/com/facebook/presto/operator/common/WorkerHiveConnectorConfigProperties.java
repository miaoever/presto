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
package com.facebook.presto.operator.common;

import com.google.common.collect.ImmutableMap;

public class WorkerHiveConnectorConfigProperties
        extends Properties
{
    public static final String CACHE_ENABLED = "cache.enabled";
    public static final String CACHE_MAX_CACHE_SIZE = "cache.max-cache-size";
    public static final String CONNECTOR_NAME = "connector.name";

    private static final String DEFAULT_CACHE_ENABLED = "true";
    private static final String DEFAULT_CACHE_MAX_CACHE_SIZE = "35433";
    private static final String DEFAULT_CONNECTOR_NAME = "hive";

    private WorkerHiveConnectorConfigProperties(PropertiesPopulator populator, ImmutableMap<String, String> propertiesMap)
    {
        super(populator, propertiesMap);
    }

    public static class WorkerHiveConnectorConfigPropertiesBuilder
            extends Properties.PropertiesBuilder<WorkerHiveConnectorConfigPropertiesBuilder>
    {
        public WorkerHiveConnectorConfigPropertiesBuilder initializeDefault()
        {
            propertiesMap.put(CACHE_ENABLED, DEFAULT_CACHE_ENABLED);
            propertiesMap.put(CACHE_MAX_CACHE_SIZE, DEFAULT_CACHE_MAX_CACHE_SIZE);
            propertiesMap.put(CONNECTOR_NAME, DEFAULT_CONNECTOR_NAME);
            return this;
        }

        @Override
        public WorkerHiveConnectorConfigPropertiesBuilder getThis()
        {
            return this;
        }
    }
}
