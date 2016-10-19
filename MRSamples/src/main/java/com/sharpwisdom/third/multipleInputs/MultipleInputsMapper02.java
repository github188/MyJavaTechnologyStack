package com.sharpwisdom.third.multipleInputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 *
 * 多文件输入执行join操作
 * hadoop 多文件格式输入，一般可以使用MultipleInputs类指定不同的输入文件路径以及输入文件格式
 * 1.需求
 * 现有两份数据:
 * phone:
 * 123,good number
 * 124,common number
 * 125,bad number
 *
 * user:
 * zhangsan,123
 * lisi,124
 * wangwu,125
 * 现在需要把user和phone按照phone number连接起来，得到下面的结果:
 * zhangsan,123,good number
 * lisi,123,common number
 * wangwu,125,bad number
 *
 */
public class MultipleInputsMapper02 extends Mapper<LongWritable,Text,Text,FlagStringDataType>{

    private String delimiter;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(line.length() > 0){
            String[] arr = line.split(delimiter);
            if(arr.length == 2){
                context.write(new Text(arr[1].trim()),new FlagStringDataType(arr[0].trim(),1));
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        delimiter = context.getConfiguration().get("delimiter",",");
    }
}
