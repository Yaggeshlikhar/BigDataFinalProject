package com.bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopAirlinesReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> top5AirlinesMap;

    @Override
    protected void setup(Reducer<NullWritable, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        this.top5AirlinesMap = new TreeMap<Integer, Text>();
    }

    @Override
    protected void reduce(NullWritable arg0, Iterable<Text> arg1,
                          Reducer<NullWritable, Text, NullWritable, Text>.Context arg2) throws IOException, InterruptedException {

        for (Text text : arg1) {
            String values[] = text.toString().split("\\s+");
            String name = values[0];
            String count = values[1];
            top5AirlinesMap.put(Integer.parseInt(count), new Text(text));
        }

        if (top5AirlinesMap.size() > 5) {
            top5AirlinesMap.remove(top5AirlinesMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Reducer<NullWritable, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for (Text text : top5AirlinesMap.values()) {
            context.write(NullWritable.get(), text);
        }
    }
}
