package ml.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NBTrain {

	/**
	 * Mapper which takes as input <byteoffset, line of text> and outputs
	 * <label, 1>.
	 * 
	 * @author
	 *
	 */
	public static class NBPriorsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase());

			word.set(tokenizer.nextToken());
			output.collect(word, one);

		}
	}

	/**
	 * Reducer which takes as input <label, 1> and outputs <label, sum of
	 * occurrences of label>.
	 * 
	 * @author
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
	 * Mapper which takes as input <byteoffset, line of text> and outputs
	 * <label-feature concatenation, 1>.
	 * 
	 * @author maddie
	 *
	 */
	public static class NBCountsReducer extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase());

			// Data structure to keep track of feature occurrences in 1 specific
			// example
			ArrayList<String> labelFeature = new ArrayList<String>();

			String label = tokenizer.nextToken(); // save the label (e.g. first
													// word in line of text)

			// For each feature, make sure that it has not already been counted
			// for this single example.
			// If it hasn't, add it as the key (concatenated with the example's
			// label) to an output with a value of 1.
			while (tokenizer.hasMoreTokens()) {
				String keyText = label + ", " + tokenizer.nextToken();
				word.set(keyText);
				if (!labelFeature.contains(keyText)) {
					output.collect(word, one);
					labelFeature.add(keyText);
				}
			}
		}
	}

}
