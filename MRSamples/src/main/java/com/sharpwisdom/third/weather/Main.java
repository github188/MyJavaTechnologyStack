package com.sharpwisdom.third.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/12.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("================开始任务!==========打印");
        LOG.info("*********************** 开始任务!*********log输出");
        Configuration configuration = new Configuration();
        Job job = new Job(configuration,"max_temperature_job");
        job.setJarByClass(Main.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
            System.out.println("output file is exist,but has deleted!");
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
