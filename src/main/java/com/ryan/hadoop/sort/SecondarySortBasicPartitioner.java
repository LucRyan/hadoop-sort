package com.ryan.hadoop.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by RyanW on 4/30/16.
 */
public class SecondarySortBasicPartitioner extends
        Partitioner<CompositeKeyWritable, NullWritable> {

    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value,
                            int numReduceTasks) {

        return (int)(key.getNum() % numReduceTasks);
    }
}