package com.sharpwisdom.third.multipleInputs;

import com.sharpwisdom.third.multiSort.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Created by wuys on 2016/8/17.
 */
public class JobMain extends Configuration implements Tool {

    private String input1 = null;
    private String input2 = null;
    private String output = null;
    private String delimiter = null;

    @Override
    public int run(String[] strings) throws Exception {
        setArgs(strings);
        checkParam();

        Configuration configuration = new Configuration();
        Job job = new Job(configuration,"multiInputs-job");
        job.setJarByClass(JobMain.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlagStringDataType.class);

        job.setReducerClass(MultipleInputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,new Path(input1), TextInputFormat.class,MultipleInputsMapper01.class);
        MultipleInputs.addInputPath(job,new Path(input2), TextInputFormat.class,MultipleInputsMapper02.class);

        Path outputDir = new Path(output);
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outputDir)){
            fs.delete(outputDir,true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);
        System.exit(job.waitForCompletion(true)?0:1);
        return 0;
    }

    private void checkParam(){
        if(input1==null || "".equals(input1.trim())){
            System.out.println(" no input phone-data path!");
            userMaunel();
            System.exit(-1);
        }
        if(input2==null || "".equals(input2.trim())){
            System.out.println(" no input user-data path!");
            userMaunel();
            System.exit(-1);
        }
        if(output==null || "".equals(output.trim())){
            System.out.println(" no output path!");
            userMaunel();
            System.exit(-1);
        }
        if(delimiter==null || "".equals(delimiter.trim())){
            System.out.println(" no delimiter!");
            userMaunel();
            System.exit(-1);
        }
    }

    private void userMaunel(){
        System.err.println("Usage:");
        System.err.println("-i1 input \t phone data path.");
        System.err.println("-i2 input \t user data path.");
        System.err.println("-o input \t output data path.");
        System.err.println("-delimiter input \t delimiter.");
    }

    //-i1 xxx -i2 xxx -o xxx -delimiter x
    private void setArgs(String[] args){
        for(int i=0;i<args.length;i++){
            if("-i1".equals(args[i])){
                input1 = args[++i];
            }
            if("-i2".equals(args[i])){
                input2 = args[++i];
            }
            if("-o".equals(args[i])){
                output = args[++i];
            }
            if("-delimiter".equals(args[i])){
                delimiter = args[++i];
            }

        }
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }
}
