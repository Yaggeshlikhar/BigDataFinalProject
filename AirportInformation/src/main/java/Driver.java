import com.bigdata.airportInformation.InputOneMapper;
import com.bigdata.airportInformation.InputReducer;
import com.bigdata.airportInformation.InputTwoMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String args[]) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: <InputOne> <InputTwo> <Output>");
            System.exit(2);
        }
        Path inputOne = new Path(args[0]);
        Path inputTwo = new Path(args[1]);
        Path outputJoin = new Path(args[2]);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Join ReduceSide");
        job.setJarByClass(Driver.class);

        MultipleInputs.addInputPath(job, inputOne, TextInputFormat.class, InputOneMapper.class);
        MultipleInputs.addInputPath(job, inputTwo, TextInputFormat.class, InputTwoMapper.class);
        job.getConfiguration().set("join.type", "inner");
        job.setReducerClass(InputReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputJoin);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputJoin))
            hdfs.delete(outputJoin, true);

        System.exit(job.waitForCompletion(true) ? 0 : 2);

    }
}
