package com.bigdata.airportInformation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InputOneMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String airlineId = values[0];

        outkey.set(airlineId);
        outvalue.set("A|" + value);
        context.write(outkey, outvalue);
    }
}
