package com.sharpwisdom.third.selfJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 *
 * 自连接join
 *
 * 1.需求分析:
 * 实例中给出child-parent(孩子-父母)表，要求输出grandchild-grandparent(孩子-爷奶）表
 * 2.原始数据:
 * file:
 *  child       parent
 *  Tom         Lucy
 *  Tom         Jack
 *  Jone        Lucy
 *  Jone        Jack
 *  Lucy        Mary
 *  Lucy        Ben
 *  Jack        Alice
 *  Jack        Jesse
 *  Terry       Alice
 *  Terry       Jesse
 *  样例输出如下:
 *  grandchild      grandparent
 *  Tom             Alice
 *  Tom             Jesse
 *  Jone            Alice
 *  Jone            Jesse
 *3.设计思路分析
 *  分析这个实例，显然是需要单表连接，连接的是左表的parent列和右表的child列，且左表和右表是同一个表。
 *
 *  连接结果中除去连接的两列就是所需要的结果-------"grandchild--grandparent"表。要用MapReduce解决这个实例。
 *  首先应该考虑如何实现表的自连接，
 *  其次是连接列的设置，
 *  最后是结果的整理.
 *
 *  考虑到MapReduce的shuffle过程会将相同的key连接在一起，所以可以将map结果的key设置成待连接的列，然后列中相同的值就自然会连接在一起了。
 *  再与最开始的分析联系起来：
 *      要连接的是左表parent列和右表的child列,且左表和右表是同一个表,所以在map阶段将读入数据分割成child和parent之后，会将parent设置成key,
 *      child设置成value进行输出，并作为左表；再将同一对child和parent中的child设置成key,parent设置成value进行输出，作用为右表。为了区分输出中的
 *      左右表，需要在输出的value中再加上左右表的信息，比如在value的String最开始处加上字符1表示左表，加上字符2表示右表。这样在map的结果中就形成了
 *      左右表，然后在shuffle过程中完成连接。reduce接收到连接的结果。其中每个key的value-list就包含了"grandchild--grandparent"关系。取出每个key的
 *      value-list进行解析，将左表中的child放入一个数组，右表中的parent放入一个数组，然后对两个数组求笛卡尔积就是最后结果了（相同的key在同一个reduce中处理)。
 *
 */
public class SelfJoinMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length() > 0){
            String[] arr = line.split("\t");
            if(arr.length == 2){
                context.write(new Text(arr[1].trim()),new Text("1_" + arr[0].trim()));//left
                context.write(new Text(arr[0].trim()),new Text("2_" + arr[1].trim()));//right
            }
        }
    }
}
