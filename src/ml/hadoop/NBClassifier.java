package ml.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * A Naive Bayes classifer.  To train this classifier you need to provide
 * count files for both the labels (count(y)) as well as the the features/
 * labels (counts(x_i, y)).  
 * 
 * The classifier only classifies text examples!
 * 
 * @author dkauchak
 *
 */
public class NBClassifier {
	// label counts
	private HashMap<Double, Integer> labelCounts;
	// map from label to a counts for that label
	private HashMap<Double, HashMap<String, Integer>> featureCounts;

	// parameters
	private double lambda = 0.01;
	private boolean onlyPositive = false;

	// data set features
	private int totalExamples = 0;
	private ArrayList<Double> labels;
	private Set<String> features;

	/**
	 * Set the lambda smoothing/prior parameter
	 * 
	 * @param lambda
	 */
	public void setLambda(double lambda){
		this.lambda = lambda;
	}
	
	/**
	 * Set whether or not to use all of the features during classification
	 * or just those present in the example
	 * 
	 * @param onlyPositive
	 */
	public void setUseOnlyPositiveFeatures(boolean onlyPositive){
		this.onlyPositive = onlyPositive;
	}

	/**
	 * "train" the classifier.  Training simply involves reading in 
	 * the precomputed counts
	 * 
	 * @param priorsFile a file containing the label counts
	 * @param countsFile a file containing the feature,label counts
	 */
	public void train(String priorsFile, String countsFile){
		loadPriorsFile(priorsFile);
		loadCountsFile(countsFile);
	}

	/**
	 * Read in the priors into labelCounts and also initialize labels
	 * 
	 * @param priorsFile
	 */
	private void loadPriorsFile(String priorsFile){
		labelCounts = new HashMap<Double, Integer>();
		labels = new ArrayList<Double>();

		try {
			BufferedReader in = new BufferedReader(new FileReader(priorsFile));

			String line = in.readLine();

			while( line != null ){
				String[] parts = line.split("\t"); // tab delimited
				double label = Double.parseDouble(parts[0]);
				int labelCount = Integer.parseInt(parts[1]);
				
				labelCounts.put(label, labelCount);
				
				// other misc. info we need to keep track of
				totalExamples += labelCount;
				labels.add(label);
				
				line = in.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Read in the feature,label counts file and also initialize
	 * the feature set.
	 * 
	 * @param countsFile
	 */
	private void loadCountsFile(String countsFile){
		featureCounts = new HashMap<Double, HashMap<String, Integer>>();
		features = new HashSet<String>();

		try {
			BufferedReader in = new BufferedReader(new FileReader(countsFile));

			String line = in.readLine();

			while( line != null ){
				String[] parts = line.split("\t"); // tab delimited
				double label = Double.parseDouble(parts[0]);
				String feature = parts[1];
				int count = Integer.parseInt(parts[2]);

				if( !featureCounts.containsKey(label)){
					featureCounts.put(label, new HashMap<String, Integer>());
				}
				
				featureCounts.get(label).put(feature, count);
				features.add(feature);
				
				line = in.readLine();
			}
			
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public double classify(String example) {
		double bestLogProb = -Double.MAX_VALUE;
		double bestLabel = -1;
		
		HashSet<String> exampleFeatures = getFeatures(example);
		System.out.println(exampleFeatures);
				
		for( double label: labels){
			double logProb = getLogProb(exampleFeatures, label);
			
			System.out.println(label + "\t" + logProb);
			
			if( logProb > bestLogProb ){
				bestLogProb = logProb;
				bestLabel = label;
			}
		}
		
		
		return bestLabel;
	}
	
	/** 
	 * Get the set of features (i.e. words) present in this example
	 * NOTE: this should just be the text of the examples and not the label
	 * 
	 * @param example
	 * @return
	 */
	private HashSet<String> getFeatures(String example){
		HashSet<String> features = new HashSet<String>();
		
		for( String word: example.split("\\s+")){
			if( !word.matches("[^a-z]+") ){
				features.add(word);
			}
		}
		return features;
	}
	
	/**
	 * Get the log probability of the example for this label using
	 * only the positive features
	 * 
	 * @param exampleFeatures
	 * @param label
	 * @return
	 */
	private double getLabelLogProbOnlyPositive(HashSet<String> exampleFeatures, double label){
		double sum = Math.log(((double)labelCounts.get(label))/totalExamples);
		
		for( String feature: exampleFeatures){
			sum += Math.log(getFeatureProb(feature, label));
		}
		
		return sum;
	}
		
	/**
	 * Get the log probability of the example for this label using all of
	 * the features
	 * 
	 * @param exampleFeatures
	 * @param label
	 * @return
	 */
	private double getLabelLogProbAllFeatures(HashSet<String> exampleFeatures, double label){
		double sum = Math.log(((double)labelCounts.get(label))/totalExamples);
		
		for( String feature: features ){
			if( exampleFeatures.contains(feature)){				
				sum += Math.log(getFeatureProb(feature, label));
			}else{
				sum += Math.log(1-getFeatureProb(feature, label));
			}
		}
		
		return sum;
	}
	
	/**
	 * Get the log probability of this example for label
	 * 
	 * @param ex
	 * @param label
	 * @return
	 */
	public double getLogProb(HashSet<String> ex, double label){
		return onlyPositive ? 
				getLabelLogProbOnlyPositive(ex, label) :
				getLabelLogProbAllFeatures(ex, label);
	}
	
	/**
	 * get the probability of this feature | the label
	 * 
	 * @param feature
	 * @param label
	 * @return
	 */
	public double getFeatureProb(String feature, double label){
		double numerator = featureCounts.get(label).containsKey(feature) ? featureCounts.get(label).get(feature) : 0;
		numerator += lambda;				
		double denominator = labelCounts.get(label) + lambda*features.size();		
		return numerator/denominator;
	}
}
