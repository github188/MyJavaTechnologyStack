package com.sharpwisdom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class WordCount {
    // mapper
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreElements()) {
                word.set(token.nextToken());
                context.write(word, one);
            }

        }

    }

    // reduce
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            System.out.println("====================运行结果为:" + sum);
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("======================开始运行!");
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");//这句话很重要，要不然会告你没有权限执行
        String HDFS_PATH = "hdfs://hadoop1:9000";
        String outputFilePath = HDFS_PATH + File.separator + "test" + File.separator + "output";
        //先创建文件夹
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), conf);
//        fileSystem.mkdirs(new Path(File.separator + "test" + File.separator + "input"));
//        fileSystem.mkdirs(new Path(File.separator + "test" + File.separator + "output"));
        fileSystem.deleteOnExit(new Path(outputFilePath));

        Job job = new Job(conf);

        String[] ioArgs = new String[]{HDFS_PATH + File.separator + "test" + File.separator + "input",
                outputFilePath};
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}