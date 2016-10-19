package com.sharpwisdom.third.averageGrade;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 * 平均成绩
 * 需求:
 *      对输入文件中数据进行平均成绩计算，输入文件中每一行为一个学生的姓名和他相应的成绩，如果有多门学科，则每门学科为一个文件
 *      要求在输出中每行中有两个间隔的数据。其中第一个代表学生的姓名，第二个代表其平均成绩
 * 原始数据:
 * 1)math:
 * 张三   88
 * 李四   99
 * 王五   66
 * 赵六   77
 * 2)china:
 * 张三   77
 * 李四   79
 * 王五   96
 * 赵六   67
 * 3)english:
 * 张三   65
 * 李四   66
 * 王五   67
 * 赵六   90
 *
 *  思路分析:
 *  Mapper是由InputFormat分解过的数据集,其中InputFormat的作用是将数据集切割成小的数据集InputSplit，每一个InputSplit将由一个Mapper处理。
 *  此外，InputFormat中还提供了一个RecordReader的实现，并将一个InputSplit解析成<key,value>对提供给了map函数,InputFormat的默认值是TextInputFormat，
 *  它针对文本文件，按行将文本切分成InputSplit,
 *  并用LineRecordReader将InputSplit解析成<key,value>对，key是行在文本中的位置，value是文件中的一行。
 *
 *  map 的结果会通过partion分发到Reducer,Reducer做完Reduce操作后,将通过以格式OutputFormat输出
 *  Mapper最终处理的结果对<key,value>会送到Reducer中进行合并,合并的时候，有相同key的键/值对则送到同一个Reducer上。
 *  Reducer是所有用户定制Reducer类的基础,它的输入是key和这个key对应的所有value的一个迭代器，同时还有Reducer的上下文。
 *  Reduce的结果由Reducer.Context的write方法输出到文件中。
 *
 *
 */
public class AvgMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length()>0){
            String[] arr = line.split("\t");
            if(arr.length == 2){
                context.write(new Text(arr[0]),new IntWritable(Integer.valueOf(arr[1])));
            }
        }
    }
}
