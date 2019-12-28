package com.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/8/22
 * @since 1.0.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    /**
     *
     * map端 输出到reduce端，按相同的key分发到同一个reduce去执行
     *  （hello，<1,1,1>）
     *     (welcome,<1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
