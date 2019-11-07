package com.bigdata.TopSourceAirports;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopMapper2 extends Mapper<LongWritable, Text, TopCustomWritable, IntWritable> {
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        Text sourceId = new Text();
        IntWritable numberOfFlights = new IntWritable();
        if (values.toString().length() > 0) {
            try {
                String[] value = values.toString().split("\t");
                sourceId.set(value[0]);
                numberOfFlights.set(Integer.parseInt(value[1]));

                TopCustomWritable data = new TopCustomWritable(sourceId.toString(), numberOfFlights.get());
                context.write(data, new IntWritable(1));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
