package decisiontree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Id3Node extends Node implements Serializable {
	private static final long serialVersionUID = 2385446682051338699L;
	// variables required for training
	private transient List<String> attributesTested;
	private transient Instances instances;
	private transient double entropy;
	private transient double purity;
	// variables required for classification
	private String classifier;
	private String attribute;
	private boolean discrete;
	private double split;
	private String value;
	private String fill;

	/**
	 * Constructor for root node
	 * 
	 * @param instances
	 */
	public Id3Node(Instances instances) {
		// create root node with no parent
		super(null);
		// set instances for this node
		setInstances(instances);
		// create attributes tested list
		attributesTested = new ArrayList<String>();
	}

	/**
	 * Constructor for child nodes
	 * 
	 * @param instances
	 * @param parent
	 */
	public Id3Node(Instances instances, List<String> attributesTested,
			Id3Node parent) {
		// create root node with parent
		super(parent);
		// set instances for this node
		setInstances(instances);
		// set attributes tested for this node
		setAttributesTested(attributesTested);
	}

	/**
	 * Getter method for instances
	 * 
	 * @return instances
	 */
	public Instances instances() {
		return instances;
	}

	/**
	 * Getter method for classifier
	 * 
	 * @return classifier
	 */
	public String classifier() {
		return classifier;
	}

	/**
	 * Getter method for attribute name
	 * 
	 * @return attribute name
	 */
	public String attribute() {
		return attribute;
	}

	/**
	 * Getter method for attribute value
	 * 
	 * @return attribute value
	 */
	public String value() {
		return value;
	}

	/**
	 * Getter method for split value
	 * 
	 * @return
	 */
	public double split() {
		return split;
	}

	/**
	 * Getter method for fill value
	 * 
	 * @return
	 */
	public String fill() {
		return fill;
	}

	/**
	 * Getter method for whether the attribute on this node contains discrete
	 * values
	 * 
	 * @return true if attribute is discrete
	 */
	public boolean discrete() {
		return discrete;
	}

	/**
	 * Getter method for whether the attribute on this node contains continuous
	 * values
	 * 
	 * @return true if attribute is continuous
	 */
	public boolean continuous() {
		return !discrete;
	}

	/**
	 * Getter method for purity
	 * 
	 * @return
	 */
	public double purity() {
		return purity;
	}

	/**
	 * Getter method for entropy
	 * 
	 * @return entropy
	 */
	public double entropy() {
		return entropy;
	}

	/**
	 * Getter method for exclusion list
	 * 
	 * @return
	 */
	public List<String> attributesTested() {
		return attributesTested;
	}

	/**
	 * Set instances for this node
	 * 
	 * @param instances
	 */
	public void setInstances(Instances instances) {
		this.instances = instances;
	}

	/**
	 * Set classifier for this node
	 * 
	 * @param classifier
	 */
	public void setClassifier(String classifier) {
		this.classifier = classifier;
	}

	/**
	 * Set attribute name for this node
	 * 
	 * @param attribute
	 */
	public void setAttribute(String attribute) {
		this.attribute = attribute;
		// set discrete flag for this attribute
		this.discrete = Instance.isDiscrete(attribute);
		// set missing value fill for this attribute
		this.fill = instances.majorityAttributeValue(attribute);
	}

	/**
	 * Set split value for this node
	 * 
	 * @param split
	 */
	public void setSplit(double split) {
		this.split = split;
	}

	/**
	 * Set attribute value for this node
	 * 
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Set purity for this node
	 * 
	 * @param purity
	 */
	public void setPurity(double purity) {
		this.purity = purity;
	}

	/**
	 * Set entropy value for this node
	 * 
	 * @param entropy
	 */
	public void setEntropy(double entropy) {
		this.entropy = entropy;
	}

	/**
	 * Set attribute exclusion list
	 */
	public void setAttributesTested(List<String> attributesTested) {
		this.attributesTested = attributesTested;
	}

	/**
	 * Optional cleanup routine to reduce tree size when serializing to file
	 */
	public void clear() {
		// clear list of attributes tested
		this.attributesTested = null;
		// clear set of instances
		this.instances = null;
		// clear child nodes
		if (this.children() != null) {
			for (Node node : this.children()) {
				if (node != null)
					((Id3Node) node).clear();
			}
		}
	}
}
