package org.chencoming;


import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.chencoming.word.WordCountMapper;
import org.chencoming.word.WordCountReducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chencoming on 2018/4/1.
 */
public class WordCountMRTest extends TestCase {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;


    @Before
    public void setUp() throws Exception {
        mapDriver = MapDriver.newMapDriver(new WordCountMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new WordCountReducer());
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver.getMapper(), reduceDriver.getReducer());
    }

    @Test
    public void testMapper() throws IOException {
        List<Pair<Text, IntWritable>> out = mapDriver.withInput(new Object(),
                new Text("hello this is my first mr")).run();
        List<Pair> result = new ArrayList<Pair>();
        result.add(new Pair(new Text("hello"), new IntWritable(1)));
        result.add(new Pair(new Text("this"), new IntWritable(1)));
        result.add(new Pair(new Text("is"), new IntWritable(1)));
        result.add(new Pair(new Text("my"), new IntWritable(1)));
        result.add(new Pair(new Text("first"), new IntWritable(1)));
        result.add(new Pair(new Text("mr"), new IntWritable(1)));
        assertEquals(result,out);
    }


    @Test
    public void testReducer() throws IOException {

        List<IntWritable> cnt = new ArrayList<IntWritable>(3);
        cnt.add(new IntWritable(1));
        cnt.add(new IntWritable(3));
        cnt.add(new IntWritable(1));

        reduceDriver.withInput(new Text("chen"), cnt)
                .withOutput(new Text("chen"),new IntWritable(5));

    }

    @Test
    public void testMapReducer() throws IOException {
        List<Pair<Text, IntWritable>> out = mapReduceDriver.withInput(new Object(),
                new Text("hello chen this is chen")).run();
        List<Pair> result = new ArrayList<Pair>();
        result.add(new Pair(new Text("chen"), new IntWritable(2)));
        result.add(new Pair(new Text("hello"), new IntWritable(1)));
        result.add(new Pair(new Text("is"), new IntWritable(1)));
        result.add(new Pair(new Text("this"), new IntWritable(1)));
        assertEquals(result,out);
    }

}
