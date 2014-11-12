package decisiontree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TrainingProgram {
	private static final Log log = LogFactory.getLog(TrainingProgram.class);

	private String[] attributeNames;
	private String classifier;
	private ArrayList<String> rawTrainingData;

	public int totalClassifications;
	public int totalMisClassifications;

	private BagOfTrees bagOfTrees;

	/**
	 * Default constructor
	 */
	public TrainingProgram() {
		totalClassifications = 0;
		totalMisClassifications = 0;
	}

	/**
	 * Constructor used if skipping the data loading from file
	 */
	public TrainingProgram(String[] attributeNames, String classifier,
			ArrayList<String> rawTrainingData) {
		this();

		this.attributeNames = attributeNames;

		this.classifier = classifier;

		this.rawTrainingData = rawTrainingData;
	}

	/**
	 * Get BagOfTrees
	 * 
	 * @return
	 */
	public BagOfTrees getBagOfTrees() {
		return bagOfTrees;
	}

	/**
	 * Get number of trees in bag
	 */
	public int getBagOfTreesSize() {
		return bagOfTrees.count();
	}

	/**
	 * Load data, randomize it, and train trees
	 */
	public void Run(String path_to_file, int dataSplit, int treeCount) {
		bagOfTrees = new BagOfTrees();

		loadData(path_to_file);

		trainTreesOnDataSplits(dataSplit, treeCount, true);
	}

	/**
	 * Randomize data and train trees
	 */
	public void Run(int dataSplit, int treeCount) {
		bagOfTrees = new BagOfTrees();

		randomizeData();

		trainTreesOnDataSplits(dataSplit, treeCount, false);
	}

	/**
	 * Serialize bag of tree
	 */
	public void Save(String path_to_file) {

		log.info("Saving the trees (bag of trees) to file: " + path_to_file);
		bagOfTrees.serializeBagToFile(path_to_file);
	}

	/**
	 * Break up raw training data into small chunks and train trees off of those
	 * chunks
	 */
	private void trainTreesOnDataSplits(int dataSplitFactor,
			int numberOfTreesEachSplit, boolean printConfusionMatrix) {

		// Break up the raw training data into small pieces that trees will be
		// trained from
		int dataForTraining = rawTrainingData.size()
				- (rawTrainingData.size() / 50);
		int dataForTesting = rawTrainingData.size() - dataForTraining;
		int dataSplit = dataForTraining / dataSplitFactor;

		double reportSplitsize = dataSplitFactor / 5;

		for (int i = 0; i < dataSplitFactor; i++) {
			if ((i % reportSplitsize) == 0) {
				log.info("Creating trees for data split "
						+ (int) ((double) i / dataSplitFactor * 100)
						+ "% complete (" + bagOfTrees.count() + " trees built)");
			}
			int fromItem = i * dataSplit;
			// In case of odd numbers, make sure we catch the last record
			int toItem = (i == dataSplitFactor - 1) ? dataForTraining
					: fromItem + dataSplit;

			List<Instance> tempInstances = parseStringToInstance(rawTrainingData
					.subList(fromItem, toItem));

			// Train trees for this sub-split of data
			trainTrees(tempInstances, numberOfTreesEachSplit);

			// TODO: temporary stop while testing...
			// if (i == 3) break;

		}

		ArrayList<String> rawTestingData = new ArrayList<String>(
				rawTrainingData.subList(0, dataForTraining));

		// Clear out the data that was used for training
		rawTrainingData.clear();

		if (printConfusionMatrix) {
			// Print confusion matrix for the data set aside for testing
			generateConfussionMatrix(rawTestingData);
		}
	}

	/**
	 * Generate a confusion matrix from the list of Instance(s)
	 */
	private void generateConfussionMatrix(List<String> instanceData) {
		log.debug("Generating confussion matrix (" + instanceData.size()
				+ " records)");

		// Map of instance classifications and their respective guessed
		// classifications
		HashMap<String, HashMap<String, Integer>> confusionMatrix = new HashMap<String, HashMap<String, Integer>>();

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
	}

	private List<Instance> parseStringToInstance(List<String> strings) {
		List<Instance> tempData = new ArrayList<Instance>(strings.size());

		RecordParser parser;

		// Create a list of Instance objects
		for (String s : strings) {
			// parse data record
			parser = new RecordParser(s);

			// add the instance attribute names and values
			tempData.add(new Instance(attributeNames, parser.values(), parser
					.classifier()));
		}
		return tempData;
	}

	private Instance parseStringToInstance(String string) {
		// parse data record
		RecordParser parser = new RecordParser(string);

		// add the instance attribute names and values
		return new Instance(attributeNames, parser.values(),
				parser.classifier());

	}

	/**
	 * Train trees from random attributes
	 */
	private void trainTrees(List<Instance> instanceList, int treeCount) {
		Id3[] trees = new Id3[treeCount];
		// Take 70% of the instances at random and train a tree from them
		int trainingSize = instanceList.size() - (instanceList.size() / 70);

		log.debug("Training trees on " + trainingSize + " data rows.");

		for (int i = 0; i < treeCount; i++) {

			Instances instances = new Instances(instanceList.subList(0,
					trainingSize - 1));

			// Instantiate new TreeTrainer using the loaded instances
			TreeTrainer treeTrainer = new TreeTrainer(instances);

			// Add the tree to the trees array
			trees[i] = treeTrainer.getTreeTrainedFromRandomAttributes();

			// Test the tree's mis-classification rate across the unused 30% of
			// instances
			testTree(trees[i],
					instanceList.subList(trainingSize, instanceList.size()));

			Collections.shuffle(instanceList);
		}

		// Add to the bag the randomly trained trees
		bagOfTrees.addTrees(trees);
	}

	/**
	 * Test a tree given a list of Instance objects, and keep track of the
	 * missclassification counts
	 */
	private void testTree(Id3 tree, List<Instance> instanceList) {
		for (Instance instance : instanceList) {
			if (!instance.classifier().equals(tree.classify(instance))) {
				totalMisClassifications++;
			}
			totalClassifications++;
		}
	}

	/**
	 * Given a file name, load the data in the rawTrainingData list, then call
	 * randomizeData() to split it up into training and testing lists
	 */
	public void loadData(String path_to_file) {

		rawTrainingData = new ArrayList<String>();

		log.info("Loading data set");
		String fileRow;
		RecordParser p;
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(path_to_file);
			reader = new BufferedReader(new InputStreamReader(fis));
			// parse header record
			p = new RecordParser(reader.readLine());
			// get attribute names from header
			attributeNames = p.values();
			log.info("Attribute names " + Arrays.toString(attributeNames));
			// get classifier name from header
			classifier = p.classifier();
			log.info("Classifier name " + classifier);
			// parse remaining records
			while ((fileRow = reader.readLine()) != null) {
				rawTrainingData.add(fileRow);
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

		log.debug("Loaded " + rawTrainingData.size() + " records");

		randomizeData();
	}

	/**
	 * Randomize all data that was loaded into the training program and add 33%
	 * of it to the testing data set and retain 66% in the training data set
	 */
	public void randomizeData() {
		Collections.shuffle(rawTrainingData);

		log.debug("Randomized training data.");
	}

	public static void main(String[] args) {
		long t = System.currentTimeMillis();

		// testBagOfTrees();
		String PATH_TO_FILE = "data/kddcup.data_2_percent.txt"; // kddcup.data_10_percent.txt
		String PATH_TO_SERIALIZED_BOT = "data/kddcup.trees";
		TrainingProgram trainingProgram = new TrainingProgram();
		trainingProgram.Run(PATH_TO_FILE, 10, 5);

		int count = trainingProgram.getBagOfTreesSize();

		System.out.println("TreeBagCount: " + count);
		System.out.println("Out of bag error rate: "
				+ trainingProgram.totalMisClassifications + " / "
				+ trainingProgram.totalClassifications);

		System.out.println("Saving forest to file...");
		trainingProgram.Save(PATH_TO_SERIALIZED_BOT);

		System.out
				.println("Runtime: "
						+ (((System.currentTimeMillis() - t) / 1000) / 60)
						+ " minutes");
	}
}
