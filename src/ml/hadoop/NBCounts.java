package ml.hadoop;

import java.io.IOException;
import java.util.ArrayList;
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
 * Class which calculates the NB counts. (e.g. output = <label-feature
 * combination, occurrences>
 * 
 * @author Maddie Gordon, Nick Reminder
 *
 */
public class NBCounts {
	/**
	 * Mapper which takes as input <byteoffset, line of text> and outputs
	 * <label-feature concatenation, 1>.
	 * 
	 * @author maddie
	 *
	 */
	public static class NBCountsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			// Split string into label and features.
			String[] labelFeatures = line.toLowerCase().split("\t");
			
			// Data structure to keep track of feature occurrences in 1 specific
			// example
			ArrayList<String> labelFeature = new ArrayList<String>();

			String label = labelFeatures[0]; // save the label (e.g. first
													// word in line of text)
			//Parse each feature in the string.
			StringParser sp = new StringParser();
			ArrayList<String> features = sp.parseFeatureLine(labelFeatures[1]);
			// For each feature, make sure that it has not already been counted
			// for this single example.
			// If it hasn't, add it as the key (concatenated with the example's
			// label) to an output with a value of 1.
			for(String f : features) {
				String keyText = label + "\t" + f;
				word.set(keyText);
				if (!labelFeature.contains(keyText)) {
					output.collect(word, one);
					labelFeature.add(keyText);
				}
			}
		}
	}

	/**
	 * Reducer which takes as input <label-feature concatenation, 1> and outputs
	 * <label-feature concatenation, total count>.
	 * 
	 * @author maddie
	 *
	 */
	public static class NBCountsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
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
	 * Run the NB count calculations.
	 * 
	 * @param input
	 *            path of file containing data
	 * @param output
	 *            path of file to contain output data
	 */
	public static void run(String input, String output) {
		JobConf conf = new JobConf(NBPriors.class);
		conf.setJobName("nbcounts");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(NBCountsMapper.class);
		conf.setCombinerClass(NBCountsReducer.class);
		conf.setReducerClass(NBCountsReducer.class);

		// specify input and output dirs
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output + "/counts"));

		JobClient client = new JobClient();

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
