package com.yinjz.mr.Wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    //		atguigu atguigu

            System.out.println(key.toString());

            // 1 获取一行
            String line = value.toString();

            // 2 切割单词
            String[] words = line.split(" ");

            // 3 循环写出
            for (String word : words) {

            // atguigu
            k.set(word);

            context.write(k, v);
            }
        }

}
