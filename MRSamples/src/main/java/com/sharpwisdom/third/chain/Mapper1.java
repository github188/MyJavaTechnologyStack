package com.sharpwisdom.third.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 *
 * 链式的MapReduce
 * Hadoop的MR作业支持链式处理,类似在一个生产牛奶的流水线上,每个阶段都有特定的任务要处理,比如提供牛奶盒，封盒，打印出厂日期，等等。
 * 通过这样一步步的分工，提高的效率。
 * 从前一个Mapper的输出结果直接重定向到下一个Mapper输入，形成一个流水线，这一点和Lucene和Solr的Filter机制是非常相似的。
 *
 * 比如处理文本中的一些禁用词，或者敏感词等等，Hadoop里的链式操作，支持的形式类似正则Map + Reduce Map * 代表着全局只有一个Reduce,但Reduce的前后可以有多
 * 个Mapper来做预处理或善后工作。
 *
 * 1.需求分析
 * 数据如下:
 * 手机   5000
 * 电脑   2000
 * 衣服   300
 * 鞋子   1200
 * 裙子   434
 * 手套   12
 * 图书   12510
 * 小商品  5
 * 小商品  3
 * 订餐    2
 *
 * 需求:
 * 在第一个Mappper过滤大于10000的数据
 * 第二个Mapper过滤100-10000的数据
 * Reduce里面进行分类汇总并输出
 * Reduce后的Mapper里过滤掉商品名长度大于3的数据
 * 预计处理完后的结果是:
 * 手套   12
 * 订餐   2
 *
 * 基于新版mr写
 *
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(line.length() > 0){
            String[] arr = line.split(" ");
            int money = Integer.valueOf(arr[1].trim());
            if(money <= 10000){
                context.write(new Text(arr[0].trim()),new IntWritable(money));
            }
        }
    }
}
