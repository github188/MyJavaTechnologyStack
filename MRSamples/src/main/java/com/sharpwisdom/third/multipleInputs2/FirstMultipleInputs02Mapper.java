package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/18.
 *
 * 多种自定义文件格式的文件输入处理
 *
 * MultipleInputs可以让MR支持多种格式输入
 * 比如我们有两种文件格式，那么我们需要有两套Record Class,RecordReader和InputFormat
 *
 * MultipleInputs需要不同的InputFormat,一种InputFormat使用一种RecordReader来读取文件并返回一种Record格式的值，这就是这三个类型的关系，也是Map过程中
 * 涉及的工具及产物.
 *
 * 1.数据准备
 * a文件
 * 1t81
 * 2t90
 * 3t100
 * 4t50
 * 5t73
 * b文件
 * 1tlilit3
 * 2txiaomingt3
 * 3tfeifeit3
 * 4tzhangsant3
 * 5tlisit3
 *
 * 需求:自定义实现InputFormat,输出key,value格式数据
 *
 */
public class FirstMultipleInputs02Mapper extends Mapper<Text,FirstClass,Text,Text>{

    @Override
    protected void map(Text key, FirstClass value, Context context) throws IOException, InterruptedException {
        context.write(key,new Text(value.toString()));
    }
}
