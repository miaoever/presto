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
package com.facebook.presto.operator;

import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.spi.UpdatablePageSource;

import java.util.Optional;
import java.util.function.Supplier;

/*
 * WrapperSourceOperator is a source operator which will delegate the read to the actual source operators wrapped inside. For example, the NativeExecutionOperator implements
 * the WrapperSourceOperator interface which will delegate the data read to the source operators running on native engine. Compared to normal source operator,
 * WrapperSourceOperator provides an additional addSplit method taking ScheduledSplit (instead of Split) which's able to associate the split to the corresponding source operator
 * (by the PlanNodeId)
 */
public interface WrapperSourceOperator
{
    Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit split);
}
