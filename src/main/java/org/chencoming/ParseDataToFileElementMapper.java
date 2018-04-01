package org.chencoming;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;


/**
 * Created by chencoming on 2018/3/25.
 */
public class ParseDataToFileElementMapper extends Mapper<Object, Text, Text, IntWritable>{

    private static final IntWritable one = new IntWritable(1);
    private Text mapKey = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        /*
         202.102.224.68|53|61.158.148.103|17872|22640|p.tencentmind.com|A|A_125.39.213.86|20160308100839.993|0|r
         处理：
         p.tencentmind.com \t 202.102.224.68 \t 61.158.148.103

         */
        String[] values = value.toString().split("\\|");

        if ("r".equals(values[10])) {

            mapKey.set(values[5] + "\t" + values[0] + "\t" + values[2]);
            System.out.println(mapKey.toString());
            context.write(mapKey, one);
        }
    }
}
