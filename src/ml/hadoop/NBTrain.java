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

import demos.WordCount;
import demos.WordCount.WordCountMapper;
import demos.WordCount.WordCountReducer;

/**
 * Class which runs the NBCounts and NBReducer MapReduce programs.
 * 
 * @author Maddie Gordon, Nick Reminder
 */
public class NBTrain {

	/**
	 * Run the MapReduce program, using the mappers and reducers in both the
	 * NBPriors class and the NBCounts class. Output into one directory.
	 * 
	 * @param args
	 *            input_directory and output_directory
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("NBTrain <input_dir> <output_dir>");
		} else {
			String input = args[0];
			String output = args[1];
			// run the NBTrain
			NBPriors.run(input, output);
			NBCounts.run(input, output);
		}
	}

}
