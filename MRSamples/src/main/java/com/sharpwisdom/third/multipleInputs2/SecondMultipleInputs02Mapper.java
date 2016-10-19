package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 */
public class SecondMultipleInputs02Mapper extends Mapper<Text,SecondClass,Text,Text> {

    @Override
    protected void map(Text key, SecondClass value, Context context) throws IOException, InterruptedException {
        context.write(key,new Text(value.toString()));
    }
}
