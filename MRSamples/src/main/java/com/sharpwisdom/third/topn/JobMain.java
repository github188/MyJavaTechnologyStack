package com.sharpwisdom.third.topn;

import com.sharpwisdom.third.sortNumber.SortNumberMapper;
import com.sharpwisdom.third.sortNumber.SortNumberReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        configuration.setInt("N",Integer.valueOf(args[2]));
        Job job = new Job(configuration,"topN-job");
        job.setJarByClass(JobMain.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(TopnMapper.class);

        job.setReducerClass(TopnReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

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
