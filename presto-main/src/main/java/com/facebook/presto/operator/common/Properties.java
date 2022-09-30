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

import java.util.HashMap;

import static java.util.Objects.requireNonNull;

public class Properties
{
    private PropertiesPopulator populator;
    private ImmutableMap<String, String> propertiesMap;

    public Properties(PropertiesPopulator populator, ImmutableMap<String, String> propertiesMap)
    {
        this.populator = populator;
        this.propertiesMap = propertiesMap;
    }

    public abstract static class PropertiesBuilder<T extends PropertiesBuilder<T>>
    {
        protected final HashMap<String, String> propertiesMap = new HashMap<>();
        private PropertiesPopulator populator;

        public T setPopulator(PropertiesPopulator populator)
        {
            this.populator = populator;
            return getThis();
        }

        public T setProperty(String propertyName, String value)
        {
            propertiesMap.put(propertyName, value);
            return getThis();
        }

        public abstract T getThis();

        public Properties build()
        {
            requireNonNull(populator, "populator is null.");
            return new Properties(populator, ImmutableMap.copyOf(propertiesMap));
        }
    }

    public void populateProperties()
    {
        populator.populate(propertiesMap);
    }
}
