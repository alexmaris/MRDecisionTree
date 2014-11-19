package decisiontree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import decisiontree.BagOfTrees;
import decisiontree.Instance;
import decisiontree.RecordParser;

public class ClassifierOfKnownData {
	private static final Log log = LogFactory
			.getLog(ClassifierOfKnownData.class);

	public String[] attributeNames;
	public BagOfTrees bagOfTrees;
	public List<String> instanceData;

	public ClassifierOfKnownData(){
		
		attributeNames = new String[]  { "#duration", "@protocol_type", "@service",
				  "@flag", "#src_bytes", "#dst_bytes", "@land", "#wrong_fragment",
				  "#urgent", "#hot", "#num_failed_logins", "@logged_in",
				  "#num_compromised", "#root_shell", "#su_attempted", "#num_root",
				  "#num_file_creations", "#num_shells", "#num_access_files",
				  "#num_outbound_cmds", "@is_host_login", "@is_guest_login", "#count",
				  "#srv_count", "#serror_rate", "#srv_serror_rate", "#rerror_rate",
				  "#srv_rerror_rate", "#same_srv_rate", "#diff_srv_rate",
				  "#srv_diff_host_rate", "#dst_host_count", "#dst_host_srv_count",
				  "#dst_host_same_srv_rate", "#dst_host_diff_srv_rate",
				  "#dst_host_same_src_port_rate", "#dst_host_srv_diff_host_rate",
				  "#dst_host_serror_rate", "#dst_host_srv_serror_rate",
				  "#dst_host_rerror_rate", "#dst_host_srv_rerror_rate" };
			}

	/**
	 * Given a list of data rows to classify, use the BagOfTrees to classify and
	 * generate a confusion matrix
	 */
	private void generateConfussionMatrix(List<String> instanceData) {
		log.debug("Generating confussion matrix (" + instanceData.size()
				+ " records)");

		// Map of instance classifications and their respective guessed
		// classifications
		HashMap<String, HashMap<String, Integer>> confusionMatrix = new HashMap<String, HashMap<String, Integer>>();
		
		int correct = 0, total =0;

		int reportSplitsize = instanceData.size() / 100;
		for (int i = 0; i < instanceData.size(); i++) {
			if ((i % reportSplitsize) == 0) {
				log.info("Classifying testing data "
						+ (int) ((double) i / instanceData.size() * 100)
						+ "% complete");
			}

			Instance instance = parseStringToInstance(instanceData.get(i));
			String guess = bagOfTrees.classifyByVote(instance);
			String classifier = instance.classifier();
			
			total++;
			if(guess.equals(classifier))
				correct++;

			// Check that the guess and classification read in is not empty
			if (!guess.equals("") || !instance.classifier().equals("")) {

				// Add classification if not currently in the collection
				if (!confusionMatrix.containsKey(classifier)) {
					confusionMatrix.put(classifier,
							new HashMap<String, Integer>());
				}

				// Add guessed classification if not currently in the collection
				if (!confusionMatrix.get(classifier).containsKey(guess)) {
					confusionMatrix.get(classifier).put(guess, 0);
				}

				// Increase the count of the guessed classification
				confusionMatrix.get(classifier).put(guess,
						confusionMatrix.get(classifier).get(guess) + 1);
			}
		}

		System.out.format("%18s", "");
		// Print the confusion matrix headers
		for (String classification : confusionMatrix.keySet()) {
			System.out.format(" %16s|", classification);
		}
		System.out.print("\n");

		// Print the confusion matrix values
		for (String classification : confusionMatrix.keySet()) {
			System.out.format("%16s |", classification);
			
			// Walk over all the possible classifications and show the guessed
			// value if a guess was made
			for (String s : confusionMatrix.keySet()) {
				String value = (confusionMatrix.get(classification)
						.containsKey(s)) ? confusionMatrix.get(classification)
						.get(s).toString() : "";
				System.out.format(" %15s |", value);
			}
			System.out.print("\n");
		}
		
		System.out.println("Accuracy: " + ((double)correct/(double)total) + " (" + correct + "/" + total + ")");
	}
	
	/**
	 * Load data that can be classified
	 */
	public List<String> loadDataFromFile(String path_to_file) {
		List<String> list = new ArrayList<String>();

		log.info("Loading testing data set");
		String fileRow;
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(path_to_file);
			reader = new BufferedReader(new InputStreamReader(fis));

			// parse records
			while ((fileRow = reader.readLine()) != null) {
				list.add(fileRow);
			}
		} catch (FileNotFoundException e) {
			System.out.println("FileNotFoundException issued");
		} catch (IOException e) {
			System.out.println("IOException issued");
		} finally {
			try {
				if (reader != null)
					reader.close();
				if (fis != null)
					fis.close();
			} catch (IOException e) {
				System.out.println("IOException when closing file");
			}
		}

		log.debug("Loaded " + list.size() + " records");
		return list;
	}

	private Instance parseStringToInstance(String string) {
		// parse data record
		RecordParser parser = new RecordParser(string);

		// add the instance attribute names and values
		return new Instance(attributeNames, parser.values(),
				parser.classifier());

	}
	
	public BagOfTrees readLocalOutput(String uri) throws Exception {

		//"hdfs://192.168.1.87:8020/ddata/output/part-r-00000";
		//"file:///home/alex/git/MRDecisionTree/MRDecisionTree/data/part-r-00000";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(uri),
				conf);

		// Key - Value (NullWritable, BytesWritable) in the sequence file
		NullWritable key = NullWritable.get();
		BytesWritable value = new BytesWritable();
		
		// Get the key and 'value' stored in the sequence file
		reader.next(key, value);

		byte[] bytes = value.getBytes();

		reader.close();

		// Deserialize the list of Id3 trees into a BagOfTree 
		BagOfTrees bagOfTrees = new BagOfTrees();
		bagOfTrees.readBagFromBytes(bytes);

		System.out.println("Number of trees:" + bagOfTrees.count());
		System.out.println("Out of bag error rate:"
				+ bagOfTrees.gettotalMisClassifications() + "/"
				+ bagOfTrees.getTotalClassifications());
		
		return bagOfTrees;
	}
	
	public static void main(String[] args) throws Exception{
		ClassifierOfKnownData cls = new ClassifierOfKnownData();
		
		// Load the serialized bag of trees from a Sequence File
		cls.bagOfTrees = cls.readLocalOutput("file:///home/alex/Downloads/200_trees");
		
		// Load the data that we want to classify
		cls.instanceData = cls.loadDataFromFile("data/corrected");		
		
		// Classify data and print a confusion matrix
		cls.generateConfussionMatrix(cls.instanceData);
	}

}
