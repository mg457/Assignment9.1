package demos;
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

public class WordCount {
	public static class WordCountMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, 
				Reporter reporter) throws IOException {
			String line = value.toString();		
			StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase());
						
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}
	
	public static class WordCountReducer extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			result.set(sum);
			output.collect(key, result);
		}
	}
	
	public static void run(String input, String output){
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		//conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);
		//conf.setReducerClass(NoOpReducer.class);
		
		// specify input and output dirs
	    FileInputFormat.addInputPath(conf, new Path(input));
	    FileOutputFormat.setOutputPath(conf, new Path(output));
		
	    JobClient client = new JobClient();
	    
	    client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}
	
	public static void main(String[] args) {
		if( args.length != 2 ){
			System.err.println("WordCount <input_dir> <output_dir>");
		}else{
			run(args[0], args[1]);
		}
	}
}