package org.chencoming.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by chencoming on 2018/4/1.
 */
public class WordCountMR {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: WordCountMR <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "WordCountMR");
        job.setJarByClass(WordCountMR.class);
        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //将reduce输出文件压缩.gz
        FileOutputFormat.setCompressOutput(job, true);  //job使用压缩
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
