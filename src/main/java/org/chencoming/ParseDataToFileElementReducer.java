package org.chencoming;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by chencoming on 2018/3/26.
 */
public class ParseDataToFileElementReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text reduceKey = new Text();
    private IntWritable result = new IntWritable();

    @Override
    /**
     * 把key相同的统计一下次数
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {


        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        this.result.set(sum);
        this.reduceKey.set("1.1-1.1" + "\t" + key.toString());  //cname + topDomain + cip + dip

        context.write(this.reduceKey, this.result);
    }
}
