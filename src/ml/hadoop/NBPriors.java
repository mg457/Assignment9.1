package ml.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Class which calculates the NBPriors (e.g. output = <label, occurrences of
 * label>).
 * 
 * @author Maddie Gordon, Nick Reminder
 * 
 */
public class NBPriors {
	/**
	 * Mapper which takes as input <byteoffset, line of text> and outputs
	 * <label, 1>.
	 *
	 */
	public static class NBPriorsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text label = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			// Split string into label and features.
			String[] labelFeatures = line.toLowerCase().split("\t");
			label.set(labelFeatures[0]);
			output.collect(label, one);
		}
	}

	/**
	 * Reducer which takes as input <label, 1> and outputs <label, sum of
	 * occurrences of label>.
	 * 
	 */
	public static class NBPriorsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	/**
	 * Run the priors calculation.
	 * 
	 * @param input
	 *            file containing data
	 * @param output
	 *            file containing outputs
	 */
	public static void run(String input, String output) {
		JobConf conf = new JobConf(NBPriors.class);
		conf.setJobName("nbpriors");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(NBPriorsMapper.class);
		conf.setCombinerClass(NBPriorsReducer.class);
		conf.setReducerClass(NBPriorsReducer.class);

		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output + "/priors"));

		JobClient client = new JobClient();

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
