package com.facebook.presto.spark.classloader_interface;

import org.apache.spark.Partition;
import org.apache.spark.shuffle.ShuffleHandle;

import java.util.Optional;

public class PrestoSparkShuffleDescriptor
{
    private final int partitionId;
    private final Optional<Partition> partition;
    private final ShuffleHandle shuffleHandle;

    public PrestoSparkShuffleDescriptor(int partitionId, Optional<Partition> partition, ShuffleHandle shuffleHandle)
    {
        this.partitionId = partitionId;
        this.partition = partition;
        this.shuffleHandle = shuffleHandle;
    }
}
