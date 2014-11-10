package decisiontree;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BagOfTrees {
	private static final Log log = LogFactory.getLog(BagOfTrees.class);

	private List<Id3> bagOfTrees;

	/**
	 * Default constructor
	 */
	public BagOfTrees() {
		bagOfTrees = new ArrayList<Id3>();
	}

	/**
	 * Add a tree to the collection
	 * 
	 * @param tree
	 */
	public void addTree(Id3 tree) {
		bagOfTrees.add(tree);
	}

	/**
	 * Add an array of trees to the collection
	 * 
	 * @param trees
	 */
	public void addTrees(Id3[] trees) {
		bagOfTrees.addAll(Arrays.asList(trees));
	}
	
	/**
	 * Add an array of trees to the collection
	 */
	public void addTrees(List<Id3> trees) {
		bagOfTrees.addAll(trees);
	}
	
	
	/**
	 * Return a List containing all Id3 trees
	 */
	public List<Id3> getTrees(){
		return bagOfTrees;
	}
	
	/**
	 * Returns the Id3 tree at the specified position in the list
	 * 
	 * @param index
	 *            position in the list
	 * @return Id3 tree
	 */
	public Id3 get(int index) {
		return bagOfTrees.get(index);
	}

	/**
	 * Returns the size of the bag of trees array
	 * 
	 * @return size of the bag of trees array
	 */
	public int count() {
		return bagOfTrees.size();
	}

	/**
	 * Serialize to a file output the list of trees that are currently held in
	 * the bag
	 * 
	 * @param filePath
	 *            file path that the bag of trees will be saved to
	 */
	public void serializeBagToFile(String filePath) {

		try {
			FileOutputStream fout = new FileOutputStream(filePath);
			ObjectOutputStream oos = new ObjectOutputStream(fout);

			oos.writeObject(bagOfTrees);
			oos.flush();
			fout.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * De-serialize from a file a list of trees
	 * 
	 * @param filePath
	 *            file path that contains bag of trees
	 */
	public void readBagFromFile(String filePath) {
		try {
			FileInputStream fin = new FileInputStream(filePath);
			ObjectInputStream ois = new ObjectInputStream(fin);

			bagOfTrees = (ArrayList<Id3>) ois.readObject();

			ois.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Vote on the most common classification for the given instance
	 * 
	 * @param instance
	 * @return
	 */
	public String classifyByVote(Instance instance) {
		HashMap<String, Integer> possibleClassifications = new HashMap<String, Integer>();

		// Use each tree in the bag to classify the instance
		for (Id3 tree : bagOfTrees) {
			String classification = tree.classify(instance);

			// If we haven't encountered this classification before, add it to
			// the map
			if (!possibleClassifications.keySet().contains(classification)) {
				possibleClassifications.put(classification, 0);
			}

			possibleClassifications.put(classification, possibleClassifications
					.get(classification).intValue() + 1);
		}

		// Return them most popular tree
		String mostPopularClassification = null;
		for (String key : possibleClassifications.keySet()) {
			if (mostPopularClassification == null) {
				mostPopularClassification = key;
			}

			mostPopularClassification = (possibleClassifications.get(key) > possibleClassifications
					.get(mostPopularClassification)) ? key
					: mostPopularClassification;
		}

		return mostPopularClassification;
	}
	
	
	/**
	 * Return the number of incorrectly classified instances
	 */
	public int getOutOfBagErrorCount(List<Instance> instanceData){
		int count = 0;
		for(Instance instance: instanceData){
			if(!instance.classifier().equals(classifyByVote(instance))){
				count++;
			}
		}
		
		return count;
	}

}
