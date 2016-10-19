package com.sharpwisdom.third.multipleInputs03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 *
 * 实战:结果输出到多个文件夹或文件中
 *
 * 技术说明:
 * MapReduce job中，可以使用FileInputFormat和FileOutputFormat来对输入和输出路径来进行设置。在输出目录中，框架自己会自动对输出文件进行命名和组织
 *如:part-(m|r)-00000之类。但有时为了后续流程的方便，我们常需要对输出结果进行一定的分类和组织。以前常用的方法是在MR job运行过后，
 * 用脚本对目录下的数据进行一次重新组织，变成我们需要的格式。研究一下MR框架中的MultipleOutputs(是2.0之后的新API，是对老版本中MultipleOutputs和MultipleOutputFormat
 * 的一个整合)
 *
 * 在一般情况下，Hadoop每个Reducer产生一个输出文件，文件以part-r-00000、part-r-00001的方式进行命名。如果需要人为控制输出文件的命名或每一个Reducer需要写出多个输出文件
 * 时，可以采用MultipleOutputs类来完成，MultipleOutputs采用输出记录的键值对(output Key和output Value)或者任意字符串来生成输出文件的名字，文件一般以
 * name-r-nnnnn的格式进行命名。
 * 其中name是程序设置的任意名字:nnnnn表示分区号。
 *
 * 需要改造Mapper或Reducer用multipleOutputs.write(key,value,baseOutputPath)代替context.write(key,value)
 *
 * 需求: 对下面这些数据按照类目输出到output中
 * 1512,iphone5s,4英寸,指纹识别,A7处理器,64位
 * 1512,iphone5,4英寸,A6处理器,IOS7
 * 50019780,ipad,9.7英寸,retina屏幕,丰富的应用
 * 50019780,yoga,联想,待机19小时,外形独特
 *
 *
 */
public class Multiple03Mapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(line.length() > 0){
            String[] arr = line.split(",");
            context.write(new Text(arr[0].trim()),value);
        }
    }
}
