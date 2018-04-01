package org.chencoming;

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
 * Hello world!
 *
 */
public class ParseDataToFileElementMR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ParseDataToFileElementMR <in> <out>");
            System.exit(2);
        }
        // hadoop1通过new方法创建job，Job.getInstance()会报"ERROR: Method Not Exception"
        Job job = new Job(conf, "ParseDataToFileElementMR");
        job.setJarByClass(ParseDataToFileElementMR.class);
        //Mapper
        job.setMapperClass(ParseDataToFileElementMapper.class);

        //Combiner
//        job.setCombinerClass(ParseDataToFileElementReducer.class);

        //Reducer
        job.setReducerClass(ParseDataToFileElementReducer.class);
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
