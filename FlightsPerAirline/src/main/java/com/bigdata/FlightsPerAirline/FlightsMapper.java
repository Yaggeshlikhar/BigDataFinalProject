package com.bigdata.FlightsPerAirline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

    private final static Text t = new Text();
    private final static IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        String [] values = value.toString().split(",");
        t.set(values[8]);
        context.write(t, intWritable);
//        for(String s : values) {
//            Text t = new Text(s);
//            context.write(t, intWritable);
//            break;
//        }
    }
}
