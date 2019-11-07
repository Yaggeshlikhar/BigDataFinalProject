package com.bigdata.TopSourceAirports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopMapper1  extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text sourceId = new Text();
        IntWritable flightCount = new IntWritable(1);
        String[] values = value.toString().split(",");

        sourceId.set(values[16]);
        context.write(sourceId, flightCount);

    }
}