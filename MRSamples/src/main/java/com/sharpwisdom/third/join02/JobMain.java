package com.sharpwisdom.third.join02;

import com.sharpwisdom.third.duplicateNumber.DuplicateNumberMapper;
import com.sharpwisdom.third.duplicateNumber.DuplicateNumberReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by wuys on 2016/8/16.
 */
public class JobMain {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = new Job(configuration,"join02-job");
        job.setJarByClass(JobMain.class);

        job.setMapperClass(Join02Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(User.class);

        job.setReducerClass(Join02Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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
