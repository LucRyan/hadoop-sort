package com.ryan.hadoop.sort;
import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by RyanW on 4/23/16.
 */
public class SortValueAscending {
    public static class Map extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long count = 0;

            while (tokenizer.hasMoreTokens()) {
                context.write(new CompositeKeyWritable(count, tokenizer.nextToken()), NullWritable.get());

                count++;
            }
        }
    }

    public static class Reduce extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
        @Override
        public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
                           Context context) throws IOException, InterruptedException {

            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {

        protected SecondarySortBasicCompKeySortComparator() {
            super(CompositeKeyWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

            return key1.getText().compareTo(key2.getText());
        }
    }

    public static void main(String[] args) throws Exception {

        if(isEmptyOrNull(args[0]) || isEmptyOrNull(args[1]))
        {
            System.out.print("Please provide input file path and output directory");
            return;
        }
        clearFolder(args[1]);

        JobConf conf = new JobConf();
        Job job = new Job(conf);

        job.setJobName("Secondary sort example");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(SecondarySortBasicPartitioner.class);
        job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(8);

        job.waitForCompletion(true);
    }

    //region Helper
    private static void clearFolder(String dirPath)
    {
        File dir = new File(dirPath);
        try{
            if(dir.isDirectory())
            {
                FileUtils.deleteDirectory(dir);
            }
        }
        catch (java.io.IOException exception)
        {
            exception.printStackTrace();
        }
    }

    private static boolean isEmptyOrNull(String s)
    {
        return  s == null || s.isEmpty();
    }
    //endregion
}