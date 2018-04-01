package org.chencoming.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by chencoming on 2018/4/1.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {


    private final IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split(" ");

        for(String w : words){
            context.write(new Text(w.toLowerCase()), intWritable);
        }

    }
}
