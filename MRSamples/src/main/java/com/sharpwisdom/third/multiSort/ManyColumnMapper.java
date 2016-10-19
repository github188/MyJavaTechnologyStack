package com.sharpwisdom.third.multiSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 *
 * 多字段排序
 * 1.原始数据
 * str1         2
 * str2         5
 * str3         9
 * str1         1
 * str2         3
 * str3         12
 * str1         8
 * str2         7
 * str3         19
 *
 * 2.预期输出
 * str1     1,2,8
 * str2     3,5,7
 * str3     9,12,19
 *
 * 3.设计思路
 *  从对Map reduce的理解我们可以轻松的知道key会在整个过程中经历排序的过程。但是我们观察输出结果可以看出value也实现了排序。因此问题的关键就是如何实现value的排序。
 *  这里我们需要引进一个自定义的key,在这个自定义的key里面实现key和value的排序工作，如何实现呢？从前面的例子我们可以知道，可以利用mapreduce的二次排序功能来实现。
 *  这里既有要同时对key和value,我们设计的自定义Key里面肯定应该包括这两部分数据，遵从我们前面的总结:
 *     核心总结:
 *          1.map最后阶段进行partition分区，一般使用job.setPartitionerClass设置的类，如果没有则用Key的hashcode()方法进行排序。
 *          2.每个分区内部调用job.setSortComparatorClass设置的key比较函数类进行排序，如果没有则使用Key的实现的compareTo方法。
 *          3.当reduce接收到map传过来的数据之后，调用job.setSortComparatorClass设置的key比较函数类对所有数据对排序，如果没有则使用Key的实现的compareTo方法
 *          4.紧接着使用job.setGroupingComaratorClass设置的分组函数类，进行分组，同一个key的value放在一个迭代器里面。
 *    针对第一点，我们需要解决符合主键的partition问题，因此这里我们会实现一个按照输出key进行分区的类，最后通过job.setPartitionerClass设置
 *    针对第二点，需要写一个比较函数的实现类，主要作用是如果key相同的情况下将value按照升序排列
 *    针对第三点，同样可以使用上面的比较函数实现类即可
 *    针对第四点，我们需要对默认的group规则进行干预，写一个自己的group实现
 *
 *
 */
public class ManyColumnMapper extends Mapper<Object,Text,IntPair,IntWritable> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String first = key.toString();
        Integer second = Integer.valueOf(value.toString().trim());
        context.write(new IntPair(first,second),new IntWritable(second));
    }
}
