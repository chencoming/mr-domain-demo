package org.chencoming;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple ParseDataToFileElementMR.
 */
public class ParseDataToFileElementMRTest
    extends TestCase {


    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;


    @Before
    public void setUp() throws Exception {
        mapDriver = MapDriver.newMapDriver(new ParseDataToFileElementMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new ParseDataToFileElementReducer());
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver.getMapper(), reduceDriver.getReducer());
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new Object(), new Text(
                "202.102.224.68|53|115.60.109.162|3760|8920|a.root-servers.net|A|A_198.41.0.4|20160308100839.993|0|r"));
        mapDriver.withOutput(new Text("a.root-servers.net\t202.102.224.68\t115.60.109.162"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(10));
        reduceDriver.withInput(new Text("a.root-servers.net\t202.102.224.68\t115.60.109.162"), values);
        reduceDriver.withOutput(new Text("1.1-1.1\ta.root-servers.net\t202.102.224.68\t115.60.109.162"),
                new IntWritable(12));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReducer() {
        mapReduceDriver.withInput(new Object(), new Text(
                "202.102.224.68|53|115.60.109.162|3760|8920|a.root-servers.net|A|A_198.41.0.4|20160308100839.993|0|r"));
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        mapReduceDriver.withOutput(new Text("1.1-1.1\ta.root-servers.net\t202.102.224.68\t115.60.109.162"), new IntWritable(1));
        mapReduceDriver.runTest();
    }


}
