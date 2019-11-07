package com.bigdata.TopSourceAirports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopReducer2 extends Reducer<TopCustomWritable, IntWritable, TopCustomWritable, NullWritable> {

    public static int count = 1;

    @Override
    protected void reduce(TopCustomWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {

            if (count <= 5) {
                context.write(key, NullWritable.get());
                count++;
            } else {
                break;
            }
        }
    }
}
