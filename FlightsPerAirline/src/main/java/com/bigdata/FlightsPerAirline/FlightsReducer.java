package com.bigdata.FlightsPerAirline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightsReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int totalAirlines = 0;
        for(IntWritable i : values) {
            totalAirlines += i.get();
        }

        intWritable.set(totalAirlines);
        context.write(key, intWritable);
    }
}

