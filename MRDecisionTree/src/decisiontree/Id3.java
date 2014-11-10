package decisiontree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Id3 implements Serializable {
	private static final long serialVersionUID = -781699850100065981L;

	private static final Log log = LogFactory.getLog(Id3.class);
	private static double log2 = Math.log(2);
	private Instances testInstances;
	private List<Instance> testInstance;
	private List<String> predicted;
	private boolean randomForest;
	private int randomForestSize;
	private Instances instances;
	private double accuracy;
	private Id3Node root;

	/**
	 * Constructor for id3
	 */
	public Id3(Instances instances) {
		this.instances = instances;
		// create the root node
		setRoot(new Id3Node(instances));
		log.info("Id3 tree created with " + instances.size() + " instances");
		// default to non-random attribute
		randomForest = false;
	}

	/**
	 * Traverse root node
	 */
	public void traverse() {
		traverse(root());
	}

	/**
	 * Traverse the node tree
	 * 
	 * @param instances
	 * @param attribute
	 * @param value
	 * @return
	 */
	public void traverse(Id3Node node) {
		log.info("Traversal node contains " + node.instances().size()
				+ " instances");
		// return if node is null
		if (node == null)
			return;
		// return if there are no instances
		if (node.instances().size() == 0) {
			log.info("Returning empty node");
			return;
		}
		;
		// compute purity for instance set
		node.setPurity(node.instances().classifierPurity());
		log.info("Node purity " + node.purity());
		// compute entropy for instance set
		node.setEntropy(computeEntropy(node.instances()));
		log.info("Node entropy " + node.entropy());
		// no further traversal if entropy is 0
		if (node.entropy() == 0) {
			node.setClassifier(node.instances().majorityClassifier());
			log.info("Node classifier " + node.classifier() + ", entropy is 0");
			return;
		}
		// no further traversal if all attributes tested
		if (!randomForest()
				&& attributesExhausted(node.instances(),
						node.attributesTested())) {
			node.setClassifier(node.instances().majorityClassifier());
			log.info("Node classifier " + node.classifier()
					+ ", attributes exhausted");
			return;
		}
		// compute attribute with maximum information gain
		if (randomForest()) {
			// compute max info gain with random sampling of attributes
			node.setAttribute(computeMaxInfoGain(node.instances(),
					randomForestAttributes(), null));
		} else {
			// compute max info gain with all instance atttributes, maintain
			// attributes tested
			node.setAttribute(computeMaxInfoGain(node.instances(),
					new ArrayList(instances.attributes()),
					node.attributesTested()));
		}
		log.info("Node attribute with max info gain " + node.attribute());
		// update attributes tested
		List<String> attributesTested = node.attributesTested();
		attributesTested.add(node.attribute());
		// determine if this node has continuous ranged values
		if (node.continuous()) {
			/**
			 * When the attribute selected for the node contains continuous
			 * ranged values, a binary split algorithm is used to split the
			 * values across left and right child nodes
			 */
			log.info("Node will traverse a binary split");
			// compute binary split
			node.setSplit(computeBinarySplit(node.instances(), node.attribute()));
			log.info("Node binary split value " + node.split());
			// split instances using binary split value
			Instances[] split = node.instances().split(node.attribute(),
					node.split());
			// when either split is null, use majority classification
			if (split[0].size() == 0) {
				node.setClassifier(node.instances().majorityClassifier());
				log.info("Node classifier " + node.classifier()
						+ ", left split null");
			} else if (split[1].size() == 0) {
				node.setClassifier(node.instances().majorityClassifier());
				log.info("Node classifier " + node.classifier()
						+ ", right split null");
			} else {
				// create child nodes
				node.setLeft(new Id3Node(split[0], attributesTested, node));
				log.info("Left node contains " + split[0].size() + " instances");
				node.setRight(new Id3Node(split[1], attributesTested, node));
				log.info("Right node contains " + split[1].size()
						+ " instances");
				// traverse child nodes
				log.info("Traversing left node");
				traverse((Id3Node) node.left());
				log.info("Traversing right node");
				traverse((Id3Node) node.right());
			}
		} else {
			/**
			 * Otherwise, it's assumed that the attribute selected for the node
			 * contains discrete values. For this case, the set of possible
			 * values for the attribute will be used to split the instances
			 * across multiple child nodes
			 */
			log.info("Node will traverse a discrete value split");
			// split instances using discrete values
			Instances[] split = node.instances().split(node.attribute());
			// get attribute value set
			Set<String> values = node.instances().values(node.attribute());
			// convert value set to array list for indexing
			List<String> indexed = new ArrayList<String>(values);
			// create child node array
			Id3Node[] children = new Id3Node[split.length];
			// create child nodes
			for (int i = 0; i < split.length; i++) {
				// create the child node
				children[i] = new Id3Node(split[i], attributesTested, node);
				// set the attribute split value
				children[i].setValue(indexed.get(i));
			}
			// add child nodes to parent
			node.add(children);
			// traverse child nodes
			for (int i = 0; i < split.length; i++) {
				traverse(children[i]);
			}
		}
	}

	/**
	 * Getter method for root node
	 * 
	 * @return
	 */
	public Id3Node root() {
		return root;
	}

	/**
	 * Set the root node for traversal
	 * 
	 * @param root
	 */
	public void setRoot(Id3Node root) {
		this.root = root;
	}

	/**
	 * Getter method for random attribute selection
	 * 
	 * @return
	 */
	public boolean randomForest() {
		return randomForest;
	}

	/**
	 * Set random attribute selection, for use in forest of trees implementation
	 * 
	 * @param size
	 */
	public void setRandomForest(int size) {
		// set random attribute selection
		randomForest = true;
		// set random attribute selection size
		randomForestSize = size;
	}

	/**
	 * Getter method for accuracy
	 * 
	 * @return
	 */
	public double accuracy() {
		return accuracy;
	}

	/**
	 * Return true if the number of attributes tested is equal or greater than
	 * the number of attributes in the instances key set
	 * 
	 * @param instances
	 * @param attributesTested
	 * @return
	 */
	private boolean attributesExhausted(Instances instances,
			List<String> attributesTested) {
		return attributesTested.size() >= instances.attributes().size();
	}

	/**
	 * Random attribute selection of the specified size
	 * 
	 * @return list of random attributes of a specified size
	 */
	private List<String> randomForestAttributes() {
		// retrieve list of attributes
		List<String> attributes = new ArrayList<String>(instances.attributes());
		// shuffle the list randomly
		Collections.shuffle(attributes);
		// return a subset of the list
		return attributes.subList(0, randomForestSize);
	}

	/**
	 * Prune the decision tree from the root node
	 */
	public void prune() {
		prune(root());
	}

	/**
	 * Prune the decision tree recursive
	 */
	public void prune(Id3Node node) {
		if (node == null)
			return;
		// prune left and right nodes
		prune((Id3Node) node.left());
		prune((Id3Node) node.right());
		// prune this node
		if (node.left() != null && node.right() != null) {
			double branchPurity = (((Id3Node) node.left()).purity() + ((Id3Node) node
					.right()).purity()) / 2;
			if (node.purity() > branchPurity) {
				log.info("Pruning purity branch " + branchPurity + " node "
						+ node.purity());
				node.setLeft(null);
				node.setRight(null);
				node.setClassifier(node.instances().majorityClassifier());
				log.info("Pruned node classifier " + node.classifier());
			}
		}
		return;
	}

	/**
	 * Test instances on trained data
	 * 
	 * @param instances
	 */
	public void test(Instances instances) {
		testInstances = instances;
		// get instances in list form
		testInstance = instances.instances();
		// allocate similar list for classifications
		predicted = new ArrayList(testInstance.size());
		// classify each instance
		int matches = 0;
		for (int j = 0; j < testInstance.size(); j++) {
			String classification = classify(testInstance.get(j));
			predicted.add(j, classification);
			log.info("Actual classification "
					+ testInstance.get(j).classifier() + ", predicted "
					+ classification);
			if (classification.equals(testInstance.get(j).classifier()))
				matches++;
		}
		// compute accuracy
		accuracy = (double) matches / (double) testInstance.size();
		log.info("Test completed with " + matches + " out of "
				+ testInstance.size() + " matches, accuracy " + accuracy);
	}

	/**
	 * Classify instance from root node
	 * 
	 * @param instance
	 * @return
	 */
	public String classify(Instance instance) {
		return classify(root(), instance);
	}

	/**
	 * Classify instance with given node
	 * 
	 * @param node
	 * @param instance
	 * @return classification for the given instance
	 */
	public String classify(Id3Node node, Instance instance) {
		// return node classification if defined
		if (node.classifier() != null)
			return node.classifier();
		// determine if the attribute on this node is continuous
		if (node.continuous()) {
			// traverse binary child nodes to get classification
			if (instance.valueDouble(node.attribute()) <= node.split()) {
				return classify((Id3Node) node.left(), instance);
			} else {
				return classify((Id3Node) node.right(), instance);
			}
		} else {
			// get missing value fill
			String fill = node.fill();
			// missing value fill node
			Id3Node fillNode = null;
			// get current attribute value for the instance
			String value = instance.value(node.attribute());
			// traverse discrete value child nodes to get classification
			for (Node inode : node.children()) {
				if (((Id3Node) inode).value().equals(value)) {
					// return subtree classification
					return classify((Id3Node) inode, instance);
				} else if (((Id3Node) inode).value().equals(fill)) {
					// save fill node in case of missing value
					fillNode = (Id3Node) inode;
				}
			}
			// return classification for fill node
			return classify(fillNode, instance);
		}
	}

	/**
	 * Compute classifier counts for each attribute value in the instance set
	 * 
	 * @param instances
	 * @param attribute
	 * @return map of values to map of classifiers to counts
	 */
	public Map<String, HashMap<String, MutableInt>> computeClassifierCounts(
			Instances instances, String attribute) {
		// instantiate map of values to hashmap of classifiers to counts
		Map<String, HashMap<String, MutableInt>> mappedCounts = new HashMap<String, HashMap<String, MutableInt>>();
		// for each instance in the set
		for (Instance instance : instances.instances()) {
			// get the attribute value
			String value = instance.value(attribute);
			// retrieve the hashmap of classifiers to counts
			HashMap<String, MutableInt> counts = mappedCounts.get(value);
			// add the hashmap if not defined
			if (counts == null) {
				counts = new HashMap<String, MutableInt>();
				mappedCounts.put(value, counts);
			}
			// retrieve classifier counter
			MutableInt count = counts.get(instance.classifier());
			if (count == null) {
				// create classifier counter if not defined
				counts.put(instance.classifier(), new MutableInt(1));
			} else {
				// increment classifier counter
				count.increment();
			}
		}
		return mappedCounts;
	}

	/**
	 * Compute entropy values for each attribute value in the set of instances
	 * 
	 * @param instances
	 * @return entropy value map
	 */
	private Map<String, Double> computeEntropies(Instances instances,
			String attribute) {
		// compute classifier counts for each attribute value
		Map<String, HashMap<String, MutableInt>> mappedCounts = computeClassifierCounts(
				instances, attribute);
		// retrieve attribute value counts
		Map<String, MutableInt> valueCounts = instances
				.attributeValueCounts(attribute);
		// instantiate map of values to entropies
		Map<String, Double> entropies = new HashMap<String, Double>();

		// for each mapped attribute value
		for (String value : mappedCounts.keySet()) {
			log.info("Compute entropy for attribute " + attribute + " value "
					+ value);
			// retrieve value count
			double valueCount = valueCounts.get(value).doubleValue();
			// retrieve classifier counts
			Map<String, MutableInt> counts = mappedCounts.get(value);
			// compute entropy across all instances
			double entropy = 0;
			for (String classifier : counts.keySet()) {
				int count = counts.get(classifier).intValue();
				if (count > 0) {
					double probability = (double) count / valueCount;
					entropy -= probability * (Math.log(probability) / log2);
				}
				log.info("Classifier " + classifier + " entropy " + entropy
						+ " on probability " + count + " / " + valueCount);
			}
			entropies.put(value, entropy);
			log.info("Entropy " + entropy + " on attribute " + attribute
					+ " value " + value);
		}
		return entropies;
	}

	/**
	 * Compute entropy for the set of instances
	 * 
	 * @param instances
	 * @return entropy value
	 */
	private double computeEntropy(Instances instances) {
		// initialize counters for classifiers
		Map<String, MutableInt> counts = instances.classifierCounts();
		// compute entropy across all instances
		double entropy = 0;
		log.info("Compute entropy for all instances");
		for (String classifier : counts.keySet()) {
			int count = counts.get(classifier).intValue();
			if (count > 0) {
				double probability = (double) count / (double) instances.size();
				entropy -= probability * (Math.log(probability) / log2);
			}
			log.info("Classifier " + classifier + " entropy " + entropy
					+ " on probability " + count + " / " + instances.size());
		}
		log.info("Entropy " + entropy);
		return entropy;
	}

	/**
	 * Compute the attribute with the maximum information gain
	 * 
	 * @param instances
	 * @param exclusion
	 * @return
	 */
	private String computeMaxInfoGain(Instances instances,
			List<String> attributes, List<String> attributesTested) {
		// get list of attributes
		String[] attribute = attributes.toArray(new String[attributes.size()]);
		// initialize
		int maxIndex = -1;
		double maxInfoGain = -1;
		// compute maximum information gain across all attributes
		for (int i = 0; i < attributes.size(); i++) {
			if (attributesTested == null
					|| !attributesTested.contains(attribute[i])) {
				log.info("Computing info gain for attribute " + attribute[i]);
				double infoGain = computeInfoGain(instances, attribute[i]);
				if (infoGain > maxInfoGain) {
					maxInfoGain = infoGain;
					maxIndex = i;
				}
			}
		}
		if (maxIndex >= 0) {
			log.info("Computed max info gain " + maxInfoGain + " on "
					+ attribute[maxIndex]);
		} else {
			log.info("Computed max info gain error");
		}

		// TODO: Check this logic, if the maxIndex was not identified, then
		// split on the first attribute.
		// This is a temp fix, the reason why this condition would occur needs
		// to be looked at
		return maxIndex >= 0 ? attribute[maxIndex] : attribute[0];
	}

	/**
	 * Compute information gain for the set of instances and given attribute
	 * 
	 * @param instances
	 * @param attribute
	 * @return
	 */
	private double computeInfoGain(Instances instances, String attribute) {
		// compute entropies
		Map<String, Double> entropies = computeEntropies(instances, attribute);
		// retrieve attribute counts
		Map<String, MutableInt> counts = instances
				.attributeValueCounts(attribute);
		// compute entropy of instance set
		double infoGain = computeEntropy(instances);
		// compute information gain across all attribute values
		for (String value : entropies.keySet()) {
			int count = counts.get(value).intValue();
			if (count > 0) {
				infoGain -= ((double) count / (double) instances.size())
						* entropies.get(value);
			}
			log.info("Info gain " + infoGain + " on " + count + " / "
					+ instances.size() + " entropy " + entropies.get(value));
		}
		log.info("Computed info gain " + infoGain + " on attribute "
				+ attribute);
		return infoGain;
	}

	/**
	 * Compute binary split value for the set of instances and given attribute
	 * 
	 * @param instances
	 * @param attribute
	 */
	private double computeBinarySplit(Instances instances, String attribute) {
		// get list of sorted values
		List<Double> values = Arrays.asList(instances.valuesDouble(attribute)
				.toArray(new Double[0]));
		Collections.sort(values);
		log.info("Compute binary split on attribute " + attribute + " with "
				+ values.size() + " values");
		// initialize values
		int pass = 0;
		double purity = 0;
		double min = values.get(0);
		double max = values.get(values.size() - 1);
		double midpoint = (min + max) / 2;
		log.info("Starting split evaluation min " + min + " max " + max
				+ " split " + midpoint);
		// loop until purity is 80% or 3 passes completed
		while (purity < 80 && pass++ < 3) {
			Instances[] split = instances.split(attribute, midpoint);
			double leftPurity = split[0].classifierPurity();
			double rightPurity = split[1].classifierPurity();
			log.info("Left split purity " + leftPurity + " right purity "
					+ rightPurity);
			if (leftPurity > rightPurity) {
				purity = leftPurity;
				max = midpoint;
			} else {
				purity = rightPurity;
				min = midpoint;
			}
			midpoint = (min + max) / 2;
			log.info("Split pass " + pass + " min " + min + " max " + max
					+ " split " + midpoint);
		}
		log.info("Computed binary split " + midpoint + " for " + attribute);
		return midpoint;
	}

	/**
	 * Print the confusion matrix
	 */
	public void printConfusionMatrix() {
		// print header
		System.out.println("Confusion Matrix:\n");
		System.out.format("%20s", "");
		for (String classifier : testInstances.classifiers()) {
			System.out.format("%20s | ", classifier);
		}
		System.out.println("\n");
		// get set of classifiers
		Set<String> classifiers = testInstances.classifiers();
		// print each classifier group
		for (String classifierGroup : classifiers) {
			// initialize counters
			Map<String, Integer> counts = new HashMap<String, Integer>();
			for (String classifier : classifiers) {
				counts.put(classifier, 0);
			}
			// compute counts for each classification
			for (int i = 0; i < testInstance.size(); i++) {
				Instance instance = testInstance.get(i);
				if (instance.classifier().equals(classifierGroup)) {
					counts.put(predicted.get(i),
							counts.get(predicted.get(i)) + 1);
				}
			}
			// print results for classifier group
			System.out.format("%20s", classifierGroup);
			for (String classifier : classifiers) {
				System.out.format("%20s | ", counts.get(classifier));
			}
			System.out.println("\n");
		}
	}

	public void clear() {
		// clear set of training instances
		this.instances = null;
		// clear test instance
		this.testInstance = null;
		// clear set of test instances
		this.testInstances = null;
		// clear training data from tree structure
		root.clear();
	}
}
