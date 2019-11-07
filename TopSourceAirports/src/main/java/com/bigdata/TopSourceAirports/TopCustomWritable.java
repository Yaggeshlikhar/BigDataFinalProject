package com.bigdata.TopSourceAirports;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopCustomWritable implements Writable, WritableComparable<TopCustomWritable> {

    private String sourceId;
    private Integer numberOfFlights;

    public TopCustomWritable() {
    }

    public TopCustomWritable(String sourceId, Integer numberOfFlights) {
        this.sourceId = sourceId;
        this.numberOfFlights = numberOfFlights;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Integer getNumberOfFlights() {
        return numberOfFlights;
    }

    public void setNumberOfFlights(Integer numberOfFlights) {
        this.numberOfFlights = numberOfFlights;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(sourceId);
        dataOutput.writeInt(numberOfFlights);
    }

    public void readFields(DataInput dataInput) throws IOException {
        sourceId = dataInput.readUTF();
        numberOfFlights = dataInput.readInt();
    }

    public int compareTo(TopCustomWritable o) {
        return -1 * (numberOfFlights.compareTo(o.numberOfFlights));
    }

    @Override
    public String toString() {
        return "Source Airport = " + sourceId + "\t" +
                ", Number of Flights = " + numberOfFlights;
    }
}
