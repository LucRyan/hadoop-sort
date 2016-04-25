package com.ryan.hadoop;
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
public class SortKeyAscending {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken()) ;
                context.write(word, new Text());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text());
        }
    }

    public static class AscendingKeyComparator extends WritableComparator {
        protected AscendingKeyComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;
            return key1.compareTo(key2);
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSortComparatorClass(AscendingKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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