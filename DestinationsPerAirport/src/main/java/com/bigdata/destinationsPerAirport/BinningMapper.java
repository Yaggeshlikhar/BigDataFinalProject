package com.bigdata.destinationsPerAirport;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
        }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");
        String origin = row[16];
//        origin = origin.replace("\"", "");
//        System.out.println(origin);

        if (origin.equalsIgnoreCase("IAD")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "IAD");
        }
        if (origin.equalsIgnoreCase("IND")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "IND");
        }
        if (origin.equalsIgnoreCase("ISP")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "ISP");
        }
        if (origin.equalsIgnoreCase("JAN")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "JAN");
        }
        if (origin.equalsIgnoreCase("JAX")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "JAX");
        }
        if (origin.equalsIgnoreCase("LAS")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "LAS");
        }
        if (origin.equalsIgnoreCase("FAT")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "FAT");
        }
        if (origin.equalsIgnoreCase("SAN")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "SAN");
        }
        if (origin.equalsIgnoreCase("GEG")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "GEG");
        }
        if (origin.equalsIgnoreCase("ONT")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "ONT");
        }
        if (origin.equalsIgnoreCase("IAH")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "IAH");
        }
        if (origin.equalsIgnoreCase("ICT")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "ICT");
        }
        if (origin.equalsIgnoreCase("ILM")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "ILM");
        }
        if (origin.equalsIgnoreCase("ITO")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "ITO");
        }
        if (origin.equalsIgnoreCase("JFK")) {
            multipleOutputs.write("bins", value, NullWritable.get(), "JFK");
        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
