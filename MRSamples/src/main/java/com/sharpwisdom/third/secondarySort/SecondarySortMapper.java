package com.sharpwisdom.third.secondarySort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 *
 * 二次排序
 *
 * 1.二次排序原理:
 *      在map阶段,使用job.setInputFormatClass定义的InputFormat将输入的数据集分割成小数据块splits,同时InputFormat提供一个RecordReader的实现,
 *      本例子中使用的是TextInputFormat,它提供的RecordReader会将文本中的字节偏移量作为key,这一行的文本作为value;
 *      这就是自定义Map的输入是<LongWritable,Text>的原因，然后调用自定义Map的map方法，将一个个<LongWritable,Text>对输入给Map的map方法。
 *      注意输出应该符合自定义map中定义的输出<IntPair,IntWritable>.最终是生成一个List<IntPair,IntWritable>.
 *      在map阶段的最后，会先调用job.setPartitionerClass对这个list进行分区，每个分区映射到一个reducer。
 *      每个分区内又调用job.setSortComparatorClass设置的key比较函数类排序。可以看到，这本身就是一个二次排序。
 *      如果没有通过job.setSortComparatorClass设置key比较函数类，则使用key实现的compareTo方法。
 *      在第一个例子中，使用了IntPair实现的compareTo方法，而在下一个例子中，专门定义了key比较函数类。
 *
 *      在reduce阶段，reducer接收到所有映射到这个reducer的map输出后，也是会调用job.setSortComparatorClass设置的key比较函数类对所有数据进行排序。
 *      然后开始构造一个key对应的value迭代器，这时就要用到分组，使用job.setGroupingComparatorClass设置的分组函数类。
 *      只要这个比较器比较的两个key相同，他们就属于同一组，它们的value放在一个value迭代器，而这个迭代器的key使用属于同一个组的所有key的第一个key,
 *      最后就是进入Reducer的reduce方法，reduce方法的输入是所有(key和它的value迭代器),同样注意输入输出的类型必须与自定义的Reducer中声明的一致。
 *
 *      核心总结:
 *          1.map最后阶段进行partition分区，一般使用job.setPartitionerClass设置的类，如果没有则用Key的hashcode()方法进行排序。
 *          2.每个分区内部调用job.setSortComparatorClass设置的key比较函数类进行排序，如果没有则使用Key的实现的compareTo方法。
 *          3.当reduce接收到map传过来的数据之后，调用job.setSortComparatorClass设置的key比较函数类对所有数据对排序，如果没有则使用Key的实现的compareTo方法
 *          4.紧接着使用job.setGroupingComaratorClass设置的分组函数类，进行分组，同一个key的value放在一个迭代器里面。
 *
 *
 *  案例分析:
 *      数据准备:
 *          假如我们现在的需求是先按cookieId排序，然后按time排序，以使按session切分日志
 *
 *          cookieId        time        url
 *          2               12:12:34    2_hao123
 *          3               09:10:34    3_baidu
 *          1               15:02:41    1_google
 *          3               22:11:34    3_souhu
 *          1               19:10:34    1_baidu
 *          2               05:02:41    1_google
 *          1               12:12:44    1_hao123
 *          2               23:23:23    3_soso
 *          3               05:02:02    2_google
 *
 *          结果为:
 *          1       15:02:41    1_google
 *          1       19:10:34    1_baidu
 *          1       12:12:44    1_hao123
 *          ---------------------------------
 *          2       05:02:41    1_google
 *          2       12:12:34    2_hao123
 *          2       23:23:23    3_soso
 *          ---------------------------------
 *          3
 *          3
 *          3
 *
 */
public class SecondarySortMapper extends Mapper<LongWritable,Text,IntPair,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        if(arr.length == 3){
            IntPair tmp = new IntPair(arr[0],arr[1]);
            context.write(tmp,value);
        }
    }
}
