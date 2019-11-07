package com.bigdata.airportInformation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class InputReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private Text text = new Text();
    private ArrayList<Text> listOne = new ArrayList<Text>();
    private ArrayList<Text> listTwo = new ArrayList<Text>();
    private String joinType = null;

    public void setup(Context context) {
        joinType = "inner";
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listOne.clear();
        listTwo.clear();
        while (values.iterator().hasNext()) {
            text = values.iterator().next();
            if ((Character.toString((char) text.charAt(0)).equals("A"))
                    && (Character.toString((char) text.charAt(1)).equals("|"))) {
                listOne.add(new Text(text.toString().substring(2)));
            }
            if ((Character.toString((char) text.charAt(0)).equals("B"))
                    && (Character.toString((char) text.charAt(1)).equals("|"))) {
                listTwo.add(new Text(text.toString().substring(2)));
            }
        }
        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if (joinType.equalsIgnoreCase("inner")) {
            if (!listOne.isEmpty() && !listTwo.isEmpty()) {
                for (Text A : listOne) {
                    for (Text B : listTwo) {
                        context.write(A, B);
                    }
                }
            }
        }
    }
}
