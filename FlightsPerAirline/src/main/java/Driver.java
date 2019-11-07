import com.bigdata.FlightsPerAirline.FlightsMapper;
import com.bigdata.FlightsPerAirline.FlightsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception{

        System.out.println("Hello");

        if (args.length != 2) {
            System.out.println("Please specify Input and otput path correctly");
            System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlightsMapper.class);

        job.setMapperClass(FlightsMapper.class);
        job.setReducerClass(FlightsReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputDir);
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(outputDir)){
            hdfs.delete(outputDir, true);
        }

        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);





    }
}