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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class FilePropertiesPopulator
        implements PropertiesPopulator
{
    private final String filePath;

    public FilePropertiesPopulator(String filePath)
    {
        this.filePath = filePath;
    }

    @Override
    public void populate(ImmutableMap<String, String> propertiesMap)
    {
        try {
            File file = new File(filePath);
            file.getParentFile().mkdirs();
            FileWriter fileWriter = new FileWriter(file);
            for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
                fileWriter.write(entry.getKey() + "=" + entry.getValue() + "\n");
            }
            fileWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
