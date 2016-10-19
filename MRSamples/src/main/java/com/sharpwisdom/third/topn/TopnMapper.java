package com.sharpwisdom.third.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wuys on 2016/8/16.
 * 需求:  求最大的k个值并排序
 *
 * 原始数据:
 * #orderid,userid,payment,productid
 * 1)file1
 * 1,9819,100,121
 * 2,8918,2000,111
 * 3,2813,1234,22
 * 4,9100,10,1101
 * 5,3210,490,111
 * 6,1298,28,1211
 * 7,1010,281,90
 * 8,1818,9000,20
 *
 * 2)file2
 * 100,3333,10,100
 * 101,9321,1000,293
 * 102,3881,701,20
 * 103,6719,910,30
 * 104,8888,11,39
 * 预测结果:(求 Top N=5 的结果)
 * 1    9000
 * 2    2000
 * 3    1234
 * 4    1000
 * 5    910
 *
 */
public class TopnMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {

    int len;
    int[] top;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(line.length() > 0){
            String[] arr = line.split(",");
            if(arr.length == 4){
                int payment = Integer.parseInt(arr[2]);
                add(payment);
            }
        }
    }

    private void add(int payment){
        top[0] = payment;
        Arrays.sort(top);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        len = context.getConfiguration().getInt("N",10);
        top = new int[len + 1];//到最后取值的时候只取后面的n个,第一个丢弃掉
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i=1;i<len;i++){//不传第一个位置上的
            context.write(new IntWritable(top[i]),new IntWritable(top[i]));
        }
    }
}
