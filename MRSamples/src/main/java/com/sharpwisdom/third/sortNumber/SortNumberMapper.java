package com.sharpwisdom.third.sortNumber;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 *
 * 需求:对输入文件中的数字进行排序，输入的每一行为一个数字，要求在输出中每行有两个间隔的数字，
 * 其中，第一个代表原始数据在原始数据集中的位次，第二个代表原始数据。
 * 1)file1:
 * 2
 * 32
 * 654
 * 32
 * 15
 * 756
 * 65223
 * 2)file2:
 * 5956
 * 22
 * 650
 * 92
 *
 * 解决思路:mapreduce中如果key是IntWritable类型的则会有默认排序;如果为String的Text类型则会按照字典顺序排序;
 *          不需要配置Combiner，因为使用map reduce就能解决问题了
 *
 */
public class SortNumberMapper extends Mapper<LongWritable,Text,IntWritable,Text>{

    private Text val = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length()>0){
            context.write(new IntWritable(Integer.valueOf(line.trim())),value);
        }
    }
}
