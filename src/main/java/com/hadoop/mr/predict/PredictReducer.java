package com.hadoop.mr.predict;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/8/22
 * @since 1.0.0
 */
public class PredictReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    static Map<String, Integer> wordMap = new HashMap<>();
    static Integer goodCount = 0;
    static Integer badCount = 0;
    static Integer correctCount = 0;

    static {
        //    1.读取hdfs的文件 ==>HDFS API
        Path input = new Path("/output_2017081119/part-r-00000");
        try {
            //获取hdfs文件系统
            FileSystem fs = null;

            fs = FileSystem.get(new URI("hdfs://192.168.199.200:8020"), new Configuration(), "hadoop");

            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);//不递归的获取文件

            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                FSDataInputStream in = fs.open(file.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = "";
                while ((line = reader.readLine()) != null) {//读取到的行不为空

                    String[] split = line.split("\\s+");
                    if (split.length == 2) {
                        wordMap.put(split[0], Integer.parseInt(split[1]));
                    }
                }
                reader.close();
                in.close();

            }

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Boolean isGood = checkIsGood(values);
        context.write(key, new Text(isGood ? "好评" : "差评"));
        //好评和差评的计数
        if (isGood) ++goodCount;
        else ++badCount;

        //判断正确的计数
        if (Integer.parseInt(key.toString()) <= 1000
                && isGood
                || Integer.parseInt(key.toString()) > 1000
                && Integer.parseInt(key.toString()) <= 2000
                && !isGood) {
            correctCount++;
        }


        //最后一条的后面输出一个计数
        if (key.toString().equals("2000")) {
            context.write(new IntWritable(2017081119), new Text("好评统计：" + goodCount));
            context.write(new IntWritable(2017081119), new Text("差评统计：" + badCount));
            context.write(new IntWritable(2017081119), new Text("预测正确率：" + correctCount / 2000.0));
        }
    }

    /**
     * 预测评价可能的结果
     *
     * @param values
     * @return
     */
    public Boolean checkIsGood(Iterable<Text> values) {

        Integer goodNum = 0;
//        Double goodFactor = 0.0;
        Double good_factor = 0.0;
        Double bad_factor = 0.0;


        //遍历获得每个关键字
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text value = iterator.next();
            //todo 一个方法，可以获取关键字对应好评行和差评行的计数
            Integer good = wordMap.get("好评_" + value);
            Integer bad = wordMap.get("差评_" + value);
            //todo 通过计数计算各个词的好评权重
            /**
             * 1. 直接计数法，计算哪个多选哪个
             * 结果490:1510
             */
//            good = good == null ? 0 : good;
//            bad = bad == null ? 0 : bad;
//            goodNum += good - bad >= 0 ? 1 : -1;//好评加1差评减1


            /**
             * 2.倍数计数法
             * 如果为null就设为1
             * 如果好评>差评，计算好评/差评向上取整
             * 如果好评<差评，计算-差评/好评向上取整
             * 如果好评=差评，计算值为0
             * 结果876:1124
             */
            good = good == null ? 1 : good + 1;
            bad = bad == null ? 1 : bad + 1;
            if (good != bad) {
                Double v = (good > bad) ? Math.ceil(good / bad) : -Math.ceil(bad / good);
                goodNum += v.intValue();
            }

            /**
             * 方法三，用浮点数计算因子
             * 854:1146
             * 0.919
             */
//            good = good == null ? 1 : good + 1;
//            bad = bad == null ? 1 : bad + 1;
//            if (good != bad) {
//                goodFactor += (good > bad) ? good.doubleValue() / bad : -bad.doubleValue() / good;
//
//            }

            /**
             * 好评差评比例和比较
             * 	好评统计：639
             * 	差评统计：1361
             * 	预测正确率：0.8065
             */
//            good = good == null ? 0 : good;
//            bad = bad == null ? 0 : bad;
//            if (good != bad) {
//                good_factor += good * 1.0 / (good + bad);
//                bad_factor += bad * 1.0 / (good + bad);
//            }


        }
        /**
         * 将为零的分配给好评，0.929
         * 将为零的分配给差评，0.9245
         */
        return goodNum >= 0;
//        return good_factor >= bad_factor;
    }


}
