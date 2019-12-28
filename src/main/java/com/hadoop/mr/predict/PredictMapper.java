package com.hadoop.mr.predict;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/8/22
 * @since 1.0.0
 */

/**
 * KEYIN:map任务读取数据的kay的类型，是每行数据起始位置的偏移量 Long
 * VALUEIN:map任务读取数据的value类型，其实就是字符串 string
 * <p>
 * hello world welcome
 * hello welcome
 * <p>
 * KEYOUT: 输出key类型 string
 * VALUEOUT: 输出value的类型 int
 * <p>
 * （word，count）
 * hadoop自定义类型能序列化和反序列化
 * Long:LongWritable
 * string:Text
 */
public class PredictMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**
         * 将测试文件进行分解  分解为（行号，关键字）格式
         */

        //把value对应的行数据按照指定的间隔符拆分开
        String[] words = value.toString().split("\t");
        //word[0]是评价（好评或者差评）
        //word[1]是评价的内容
        IntWritable lineCount = new IntWritable(PredictApp.lineCount++);
        //过滤一下有些评价后面没有关键字
        if (words.length == 2) {
            String[] pjs = words[1].split(" ");
            for (String pj : pjs) {
                //如果含有非中文的就过滤掉
                if (isAllChinese(pj)) {
                    context.write(lineCount, new Text(pj));
                }
            }

        }

    }


    /**
     * 判断字符串是否全为中文
     *
     * @param str
     * @return
     */
    public boolean isAllChinese(String str) {

        if (str == null) {
            return false;
        }
        for (char c : str.toCharArray()) {
            if (!isChinese(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断单个字符是否为中文
     *
     * @param c
     * @return
     */
    public Boolean isChinese(char c) {
        return c >= 0x4E00 && c <= 0x9Fa5;
    }


}
