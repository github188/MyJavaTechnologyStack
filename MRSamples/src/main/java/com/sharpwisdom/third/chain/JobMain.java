package com.sharpwisdom.third.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by wuys on 2016/8/17.
 */
public class JobMain {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = new Job(configuration,"chain-job");
        job.setJarByClass(JobMain.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        ChainMapper.addMapper(job,Mapper1.class,LongWritable.class,Text.class,Text.class,IntWritable.class,new Configuration());
        ChainMapper.addMapper(job,Mapper2.class,Text.class,IntWritable.class,Text.class,IntWritable.class,new Configuration());
        ChainReducer.setReducer(job,OnlyReducer.class,Text.class,IntWritable.class,Text.class,IntWritable.class,new Configuration());
        ChainMapper.addMapper(job,Mapper3.class,Text.class,IntWritable.class,Text.class,IntWritable.class,new Configuration());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outputDir)){
            fs.delete(outputDir,true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
