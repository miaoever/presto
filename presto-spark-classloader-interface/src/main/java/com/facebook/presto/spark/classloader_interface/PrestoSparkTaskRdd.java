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

import org.apache.spark.MapOutputTracker;
import org.apache.spark.Partition;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.rdd.ShuffledRDDPartition;
import org.apache.spark.rdd.ZippedPartitionsBaseRDD;
import org.apache.spark.rdd.ZippedPartitionsPartition;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.ImmutableMap;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.spark_project.guava.base.Preconditions.checkArgument;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * PrestoSparkTaskRdd represents execution of Presto stage, it contains:
 * - A list of shuffleInputRdds, each of the corresponding to a child stage.
 * - An optional taskSourceRdd, which represents ALL table scan inputs in this stage.
 * <p>
 * Table scan is present when joining a bucketed table with an unbucketed table, for example:
 * Join
 * /  \
 * Scan  Remote Source
 * <p>
 * In this case, bucket to Spark partition mapping has to be consistent with the Spark shuffle partition.
 * <p>
 * When the stage partitioning is SINGLE_DISTRIBUTION and the shuffleInputRdds is empty,
 * the taskSourceRdd is expected to be present and contain exactly one empty partition.
 * <p>
 * The broadcast inputs are encapsulated in taskProcessor.
 */
public class PrestoSparkTaskRdd<T extends PrestoSparkTaskOutput>
        extends ZippedPartitionsBaseRDD<Tuple2<MutablePartitionId, T>>
{
    private List<String> shuffleInputFragmentIds;
    private List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds;
    private PrestoSparkTaskSourceRdd taskSourceRdd;
    private PrestoSparkTaskProcessor<T> taskProcessor;
    private boolean isNativeExecutionEnabled;

    public static <T extends PrestoSparkTaskOutput> PrestoSparkTaskRdd<T> create(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            // fragmentId -> RDD
            Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
            PrestoSparkTaskProcessor<T> taskProcessor,
            boolean isNativeExecutionEnabled)
    {
        requireNonNull(context, "context is null");
        requireNonNull(taskSourceRdd, "taskSourceRdd is null");
        requireNonNull(shuffleInputRddMap, "shuffleInputRdds is null");
        requireNonNull(taskProcessor, "taskProcessor is null");
        List<String> shuffleInputFragmentIds = new ArrayList<>();
        List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = new ArrayList<>();
        for (Map.Entry<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> entry : shuffleInputRddMap.entrySet()) {
            shuffleInputFragmentIds.add(entry.getKey());
            shuffleInputRdds.add(entry.getValue());
        }
        return new PrestoSparkTaskRdd<>(context, taskSourceRdd, shuffleInputFragmentIds, shuffleInputRdds, taskProcessor, isNativeExecutionEnabled);
    }

    private PrestoSparkTaskRdd(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            List<String> shuffleInputFragmentIds,
            List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
            PrestoSparkTaskProcessor<T> taskProcessor,
            boolean isNativeExecutionEnabled)
    {
        super(context, getRDDSequence(taskSourceRdd, shuffleInputRdds), false, fakeClassTag());
        this.shuffleInputFragmentIds = shuffleInputFragmentIds;
        this.shuffleInputRdds = shuffleInputRdds;
        // Optional is not Java Serializable
        this.taskSourceRdd = taskSourceRdd.orElse(null);
        this.taskProcessor = context.clean(taskProcessor, true);
        this.isNativeExecutionEnabled = isNativeExecutionEnabled;
    }

    private static Seq<RDD<?>> getRDDSequence(Optional<PrestoSparkTaskSourceRdd> taskSourceRdd, List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds)
    {
        List<RDD<?>> list = new ArrayList<>(shuffleInputRdds);
        taskSourceRdd.ifPresent(list::add);
        return asScalaBuffer(list).toSeq();
    }

    private ShuffleHandle getShuffleHandle(RDD<?> rdd)
    {
        checkArgument(rdd instanceof ShuffledRDD);
        return ((ShuffleDependency<?, ?, ?>) rdd.dependencies().head()).shuffleHandle();
    }

    private int getNumberOfPartitions(RDD<?> rdd)
    {
        checkArgument(rdd instanceof ShuffledRDD);
        return rdd.getNumPartitions();
    }

    private List<String> getBlockIds(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        List<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapSizes = seqAsJavaList(mapOutputTracker.getMapSizesByExecutorId(shuffleHandle.shuffleId(), partition.idx()));
        // return mapSizes.stream().map(item -> java.lang.Long.valueOf(item._1.executorId())).collect(Collectors.toList());
        return mapSizes.stream().map(item -> item._1.executorId()).collect(Collectors.toList());
    }

    private Map<String, PrestoSparkShuffleReadDescriptor> getShuffleReadDescriptor(Partition split, List<Partition> partitions)
    {
        ImmutableMap.Builder<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors = ImmutableMap.builder();

        // Get shuffle information from ShuffledRdds for shuffle read
        for (int inputIndex = 0; inputIndex < shuffleInputRdds.size(); inputIndex++) {
            Partition partition = partitions.get(inputIndex);
            ShuffleHandle handle = getShuffleHandle(shuffleInputRdds.get(inputIndex));
            checkArgument(partition instanceof ShuffledRDDPartition, "partition is required to be ShuffledRddPartition");
            shuffleReadDescriptors.put(
                    shuffleInputFragmentIds.get(inputIndex),
                    new PrestoSparkShuffleReadDescriptor(partition, handle, getNumberOfPartitions(shuffleInputRdds.get(inputIndex)), getBlockIds(((ShuffledRDDPartition) partition), handle)));
        }

        return shuffleReadDescriptors.build();
    }

    private List<PrestoSparkShuffleWriteDescriptor> getShuffleWriteDescriptor(Partition split)
    {
        ImmutableList.Builder<PrestoSparkShuffleWriteDescriptor> shuffleWriteDescriptors = ImmutableList.builder();

        // Get shuffle information from Spark shuffle manager for shuffle write
        checkArgument(SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager, "Native execution requires to use PrestoSparkNativeExecutionShuffleManager");
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(split.index());
        shuffleHandle.ifPresent(handle -> shuffleWriteDescriptors.add(new PrestoSparkShuffleWriteDescriptor(handle, shuffleManager.getNumOfPartitions(handle.shuffleId()))));

        return shuffleWriteDescriptors.build();
    }

    private static <T> ClassTag<T> fakeClassTag()
    {
        return scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    }

    @Override
    public Iterator<Tuple2<MutablePartitionId, T>> compute(Partition split, TaskContext context)
    {
        List<Partition> partitions = seqAsJavaList(((ZippedPartitionsPartition) split).partitions());
        int expectedPartitionsSize = (taskSourceRdd != null ? 1 : 0) + shuffleInputRdds.size();
        if (partitions.size() != expectedPartitionsSize) {
            throw new IllegalArgumentException(format("Unexpected partitions size. Expected: %s. Actual: %s.", expectedPartitionsSize, partitions.size()));
        }

        Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputIterators = new HashMap<>();
        for (int inputIndex = 0; inputIndex < shuffleInputRdds.size(); inputIndex++) {
            shuffleInputIterators.put(
                    shuffleInputFragmentIds.get(inputIndex),
                    shuffleInputRdds.get(inputIndex).iterator(partitions.get(inputIndex), context));
        }

        Iterator<SerializedPrestoSparkTaskSource> taskSourceIterator;
        if (taskSourceRdd != null) {
            taskSourceIterator = taskSourceRdd.iterator(partitions.get(partitions.size() - 1), context);
        }
        else {
            taskSourceIterator = emptyScalaIterator();
        }

        return taskProcessor.process(
                taskSourceIterator,
                unmodifiableMap(shuffleInputIterators),
                isNativeExecutionEnabled ? getShuffleReadDescriptor(split, partitions) : ImmutableMap.of(),
                isNativeExecutionEnabled ? getShuffleWriteDescriptor(split) : ImmutableList.of());
    }

    @Override
    public void clearDependencies()
    {
        super.clearDependencies();
        shuffleInputFragmentIds = null;
        shuffleInputRdds = null;
        taskSourceRdd = null;
        taskProcessor = null;
    }
}
