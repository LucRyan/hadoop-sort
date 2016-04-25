package com.ryan.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by RyanW on 4/23/16.
 */
public class ValidateResult {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text countKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int count = 0;
            while (tokenizer.hasMoreTokens()) {
                countKey.set(String.valueOf(count));
                word.set(tokenizer.nextToken()) ;
                context.write(countKey, word);

                count++;
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean valid = true;
            Text previous = null;

            for (Iterator<Text> i = values.iterator(); i.hasNext();)
            {
                Text current = i.next();
                if(previous != null)
                {
                    valid = compare(previous, current) < 0;
                }

                previous = new Text(current);

                if(!valid)
                {
                    throw new InterruptedIOException("Validation Failed");
                }
            }

            System.out.print("Validation Succeed!");
        }

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
        catch (IOException exception)
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