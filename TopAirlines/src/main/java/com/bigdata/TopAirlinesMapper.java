package com.bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopAirlinesMapper extends Mapper<Object, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> top5AirlinesMap;

    @Override
    protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        this.top5AirlinesMap = new TreeMap<Integer, Text>();
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {

        String values[] = value.toString().split("\\s+");
        try {
            String count = values[1];
            top5AirlinesMap.put(Integer.parseInt(count), new Text(value));
        } catch (Exception e) {
        }

        if (top5AirlinesMap.size() > 5) {
            top5AirlinesMap.remove(top5AirlinesMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Mapper<Object, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for (Text text : top5AirlinesMap.values()) {
            context.write(NullWritable.get(), text);
        }
    }
}