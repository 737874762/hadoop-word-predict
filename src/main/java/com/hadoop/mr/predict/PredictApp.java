package com.hadoop.mr.predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/8/22
 * @since 1.0.0
 */
public class PredictApp {


    static Integer lineCount = 1;

        public static void main(String[] args) throws Exception {
    //改为构造方法 方便调用
//    public PredictApp() throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.199.200:8020");


        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数: 主类
        job.setJarByClass(PredictApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reducer处理类
        job.setMapperClass(PredictMapper.class);
        job.setReducerClass(PredictReducer.class);

        // 设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Job对应的参数: Reduce输出key和value的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 如果输出目录已经存在，则先删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.199.200:8020"), configuration, "hadoop");
        Path outputPath = new Path("/predict_output_2017081119");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 设置Job对应的参数: Mapper输出key和value的类型：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/predict_input_2017081119"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }


}
