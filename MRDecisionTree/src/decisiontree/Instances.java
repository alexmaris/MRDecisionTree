package decisiontree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Instances implements Serializable {
	private static final long serialVersionUID = 5353526000976388264L;
	private static final Log log = LogFactory.getLog(Instances.class);
	// attributes derived from instances
	private Map<String, Attribute> attributes;
	// classifiers derived from instances
	private Map<String, MutableInt> classifiers;
	// filtered list of attributes
	private Set<String> filters;
	// instances for the set of data
	private transient List<Instance> instances;

	/**
	 * Default constructor for instances
	 */
	public Instances() {
		// instantiate list of instances
		instances = new LinkedList<Instance>();
		// instantiate mapped attributes
		attributes = new HashMap<String, Attribute>();
		// instantiate mapped classifiers
		classifiers = new HashMap<String, MutableInt>();
		// reset attributes filter
		filters = new HashSet<String>();
	}

	/**
	 * Constructor for instances given an input file
	 * 
	 * @param f
	 */
	public Instances(File f) {
		this();
		// check for null file
		if (f == null)
			return;
		// load file containing data set
		log.info("Instances constructor");
		loadDataset(f);
	}

	/**
	 * Constructor for instances given another set of instances and a filter
	 * attribute set
	 * 
	 * @param master
	 * @param filters
	 */
	public Instances(Instances instances, Set<String> filters) {
		this();
		// set filter attributes
		this.filters = filters;
		// add all instances to this set
		for (Instance instance : instances.instances()) {
			add(instance);
		}
	}

	/**
	 * Constructor for instances given a list of Instance objects
	 * 
	 * @param intanceObjects
	 */
	public Instances(List<Instance> intanceObjects) {
		this();

		for (Instance instance : intanceObjects) {
			add(instance);
		}
	}

	/**
	 * Add instance to the linked list given an array of names and values
	 * 
	 * @param names
	 * @param values
	 * @param classifier
	 */
	private void add(String[] names, String[] values, String classifier) {
		add(new Instance(names, values, classifier));
	}

	/**
	 * Add all instances to the linked list
	 * 
	 * @param instances
	 */
	public void addAll(Instances instances) {
		// add all instances to this set
		for (Instance instance : instances.instances()) {
			add(instance);
		}
	}

	/**
	 * Add instance to the linked list
	 * 
	 * @param instance
	 */
	public void add(Instance instance) {
		// add instance
		instances.add(instance);
		// update mapped attributes
		mapAttributes(instance);
		// update mapped classifiers
		mapClassifiers(instance);
	}

	/**
	 * Update attributes map and value counters
	 * 
	 * @param instance
	 */
	private void mapAttributes(Instance instance) {
		// for each attribute defined on this instance
		for (String attribute : instance.attributes()) {
			// map filtered attributes, or all if no filter defined
			if (filtered(attribute)) {
				// retrieve mapped attribute
				Attribute mappedAttribute = attributes.get(attribute);
				if (mappedAttribute == null) {
					// create mapping if none defined
					attributes
							.put(attribute,
									new Attribute(attribute, instance
											.value(attribute)));
				} else {
					// add attribute value
					mappedAttribute.add(instance.value(attribute));
				}
			}
		}
	}

	/**
	 * Update classifier map and counter
	 * 
	 * @param instance
	 */
	private void mapClassifiers(Instance instance) {
		// retrieve mutable counter
		MutableInt count = classifiers.get(instance.classifier());
		if (count == null) {
			// create new mutable counter if not mapped
			classifiers.put(instance.classifier(), new MutableInt(1));
		} else {
			// increment mutable counter
			count.increment();
		}
	}

	/**
	 * Getter to determine if an attribute is filtered for this instance set
	 * 
	 * @param attribute
	 * @return true if the set is unfiltered or the attribute is defined in the
	 *         filter, false otherwise
	 */
	private boolean filtered(String attribute) {
		if (filters == null || filters.isEmpty() || filters.contains(attribute)) {
			// when no filter defined or filter contains attribute name
			return true;
		} else {
			// when filter does not contain attribute name
			return false;
		}
	}

	/**
	 * Getter method for the map of attributes to values
	 * 
	 * @return map of attributes
	 */
	public Map<String, Attribute> attributesmap() {
		return attributes;
	}

	/**
	 * Getter method for the set of attributes on all instances
	 * 
	 * @return
	 */
	public Set<String> attributes() {
		return attributes.keySet();
	}

	/**
	 * Getter method for the set of values of a specific attribute on all
	 * instances
	 * 
	 * @param name
	 * @return set of string values
	 */
	public Set<String> values(String name) {
		return attributes.get(name).values();
	}

	/**
	 * Getter method for the set of values of a specific attribute on all
	 * instances
	 * 
	 * @param name
	 * @return set of double values
	 */
	public Set<Double> valuesDouble(String name) {
		Set<Double> values = new LinkedHashSet<Double>();
		for (Instance instance : instances) {
			values.add(instance.valueDouble(name));
		}
		return values;
	}

	/**
	 * Getter method for size of instance set
	 * 
	 * @return size of instance set
	 */
	public int size() {
		return instances.size();
	}

	/**
	 * Getter method for list of instances
	 * 
	 * @return
	 */
	public List<Instance> instances() {
		return instances;
	}

	/**
	 * Getter for enumeration of instances
	 * 
	 * @return enumeration of instances
	 */
	public Enumeration enumerate() {
		return Collections.enumeration(instances);
	}

	/**
	 * Getter method for the set of all classifiers
	 * 
	 * @return
	 */
	public Set<String> classifiers() {
		return classifiers.keySet();
	}

	/**
	 * Compute classifier counts for the set of instances
	 * 
	 * @param instances
	 * @return map of classifiers and counts
	 */
	public Map<String, MutableInt> classifierCounts() {
		return classifiers;
	}

	/**
	 * Compute the majority classifier for the set of instances
	 * 
	 * @param instances
	 * @return majority classifier
	 */
	public String majorityClassifier() {
		// initialize counters for classifiers
		Map<String, MutableInt> counts = classifierCounts();
		// initialize
		MutableInt maxCount = new MutableInt(0);
		String maxClassifier = "";
		// compute majority count
		for (String classifier : counts.keySet()) {
			if (counts.get(classifier).compareTo(maxCount) > 0) {
				maxCount = counts.get(classifier);
				maxClassifier = classifier;
			}
		}
		return maxClassifier;
	}

	/**
	 * Compute attribute value counts for the set of instances and a given
	 * attribute
	 * 
	 * @param attribute
	 * @return map of attribute names and their counts
	 */
	public Map<String, MutableInt> attributeValueCounts(String attribute) {
		// check if attribute name exists
		if (!attributes.containsKey(attribute)) {
			log.info("Unable to determine attribute counts, attribute "
					+ attribute + " not found");
			return null;
		}
		return attributes.get(attribute).counts();
	}

	/**
	 * Compute the majority attribute value for the set of instances and a given
	 * attribute
	 * 
	 * @param attribute
	 * @return
	 */
	public String majorityAttributeValue(String attribute) {
		// check if attribute name exists
		if (!attributes.containsKey(attribute)) {
			log.info("Unable to determine majority attribute value, attribute "
					+ attribute + " not found");
			return null;
		}
		// initialize counters for values
		Map<String, MutableInt> counts = attributeValueCounts(attribute);
		// initialize
		MutableInt maxCount = new MutableInt(0);
		String maxValue = "";
		// compute majority count
		for (String value : counts.keySet()) {
			if (counts.get(value).compareTo(maxCount) > 0) {
				maxCount = counts.get(value);
				maxValue = value;
			}
		}
		return maxValue;
	}

	/**
	 * Compute the percent purity of the majority classifier
	 * 
	 * @param instances
	 * @return majority classifier count
	 */
	public double classifierPurity() {
		// initialize counters for classifiers
		Map<String, MutableInt> counts = classifierCounts();
		// initialize
		MutableInt maxCount = new MutableInt(0);
		// compute majority count
		for (String classifier : counts.keySet()) {
			if (counts.get(classifier).compareTo(maxCount) > 0) {
				maxCount = counts.get(classifier);
			}
		}
		return (maxCount.doubleValue() / (double) size()) * 100;
	}

	/**
	 * Split instances given a number of subsets
	 * 
	 * @param k
	 * @return array of instances
	 */
	public Instances[] split(int k) {
		// check if k is sufficiently large
		if (k <= 1) {
			log.info("Unable to perform split subsets, number of subsets too small");
			return null;
		}
		// allocate array of instances for size of value set
		Instances[] split = new Instances[k];
		// instantiate instances for each value
		for (int i = 0; i < k; i++) {
			split[i] = new Instances();
		}
		;
		// split instances using round robin approach
		int group = 0;
		for (Instance instance : instances) {
			split[group++].add(instance);
			if (group == k)
				group = 0;
		}
		return split;
	}

	/**
	 * Split instances given an attribute name using discrete values
	 * 
	 * @param attribute
	 * @return array of instances
	 */
	public Instances[] split(String attribute) {
		// check if attribute name exists
		if (!attributes.containsKey(attribute)) {
			log.info("Unable to split instances, attribute " + attribute
					+ " not found");
			return null;
		}
		// get attribute value set
		Set<String> values = values(attribute);
		// allocate array of instances for size of value set
		Instances[] split = new Instances[values.size()];
		// instantiate instances for each value
		for (int i = 0; i < values.size(); i++) {
			split[i] = new Instances();
		}
		;
		// convert value set to array list for indexing
		List<String> indexed = new ArrayList<String>(values);
		// split instances using indexed attribute value
		for (Instance instance : instances) {
			split[indexed.indexOf(instance.value(attribute))].add(instance);
		}
		return split;
	}

	/**
	 * Split instances given an attribute and value using continuous values
	 * 
	 * @param attribute
	 * @param value
	 * @return
	 */
	public Instances[] split(String attribute, Double value) {
		// check if attribute name exists
		if (!attributes.containsKey(attribute)) {
			log.info("Unable to split instances, attribute " + attribute
					+ " not found");
			return null;
		}
		// binary split
		int k = 2;
		// allocate array of instances for size of value set
		Instances[] split = new Instances[k];
		// instantiate instances for each value
		for (int i = 0; i < k; i++) {
			// instantiate with existing attributes map
			split[i] = new Instances();
		}
		;
		// split instances using indexed attribute value
		for (Instance instance : instances) {
			if ((Double) instance.valueDouble(attribute) <= value) {
				split[0].add(instance);
			} else {
				split[1].add(instance);
			}
		}
		return split;
	}

	/**
	 * Select a random sample of instances
	 * 
	 * @param count
	 * @return random sample of instances
	 */
	public List<Instance> sample(int count) {
		// instantiate sample instances container
		List<Instance> sample = new ArrayList<Instance>(count);
		// get domain of random values
		int domain = instances.size();
		// setup random number generator
		Random random = new Random();
		// select random sample of instances
		for (int n = 0; n < count; n++) {
			int m = random.nextInt(domain);
			sample.add(instances.get(m));
		}
		return sample;
	}

	/**
	 * Merge all instances within the array, excluding the given index
	 * 
	 * @param instances
	 * @param index
	 * @return
	 */
	public static Instances merge(Instances[] instances, int index) {
		Instances merged = new Instances();
		for (int i = 0; i < instances.length; i++) {
			if (i != index)
				merged.addAll(instances[i]);
		}
		return merged;
	}

	/**
	 * Load the data set records and create all instances
	 * 
	 * @param f
	 */
	public void loadDataset(File f) {
		log.info("Loading data set");
		String r;
		RecordParser p;
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(f);
			reader = new BufferedReader(new InputStreamReader(fis));
			// parse header record
			p = new RecordParser(reader.readLine());
			// get attribute names from header
			String[] names = p.values();
			log.info("Attribute names " + Arrays.toString(names));
			// get classifier name from header
			String classifier = p.classifier();
			log.info("Classifier name " + classifier);
			// parse remaining records
			while ((r = reader.readLine()) != null) {
				// parse data record
				p = new RecordParser(r);
				// add the instance attribute names and values
				add(names, p.values(), p.classifier());
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
	}
}
