package ml.hadoop;

import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Just a vessel for the parseFeatureLine function
 * 
 * @author dkauchak
 *
 */
public class StringParser {
	
	/**
	 * Given a line of text WITHOUT the label, return the words in this
	 * line after splitting them up and removing noisy words
	 * 
	 * @param line
	 * @return
	 */
	public static ArrayList<String> parseFeatureLine(String line){
		ArrayList<String> words = new ArrayList<String>();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while( tokenizer.hasMoreTokens() ){
			String word = tokenizer.nextToken();
			
			if( !word.matches("[^a-z]+") ){
				words.add(word);
			}
		}
		
		return words;
	}
}
