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

public class WorkerNodeProperties
        extends Properties
{
    public static final String NODE_ENVIRONMENT = "node.environment";
    public static final String NODE_ID = "node.id";
    public static final String NODE_LOCATION = "node.location";
    public static final String NODE_IP = "node.ip";
    public static final String NODE_MEMORY_GB = "node.memory_gb";
    public static final String SMC_SERVICE_INVENTORY_TIER = "smc-service-inventory.tier";

    private static final String DEFAULT_NODE_ENVIRONMENT = "Sapphire-Velox";
    private static final String DEFAULT_NODE_ID = "0";
    private static final String DEFAULT_NODE_LOCATION = "/dummy/location";
    private static final String DEFAULT_NODE_IP = "0.0.0.0";
    private static final String DEFAULT_NODE_MEMORY_GB = "10";
    private static final String DEFAULT_SMC_SERVICE_INVENTORY_TIER = "dummy.tier";

    private WorkerNodeProperties(PropertiesPopulator populator, ImmutableMap<String, String> propertiesMap)
    {
        super(populator, propertiesMap);
    }

    public static class WorkerNodePropertiesBuilder
            extends Properties.PropertiesBuilder<WorkerNodePropertiesBuilder>
    {
        public WorkerNodePropertiesBuilder initializeDefault()
        {
            propertiesMap.put(NODE_ENVIRONMENT, DEFAULT_NODE_ENVIRONMENT);
            propertiesMap.put(NODE_ID, DEFAULT_NODE_ID);
            propertiesMap.put(NODE_LOCATION, DEFAULT_NODE_LOCATION);
            propertiesMap.put(NODE_IP, DEFAULT_NODE_IP);
            propertiesMap.put(NODE_MEMORY_GB, DEFAULT_NODE_MEMORY_GB);
            propertiesMap.put(SMC_SERVICE_INVENTORY_TIER, DEFAULT_SMC_SERVICE_INVENTORY_TIER);
            return this;
        }

        @Override
        public WorkerNodePropertiesBuilder getThis()
        {
            return this;
        }
    }
}
