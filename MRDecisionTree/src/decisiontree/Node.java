package decisiontree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node implements Serializable{
	private static final long serialVersionUID = 8142468351370017145L;
	private ArrayList<Node> children;
	private Node parent;
	private boolean binary;

	public Node(Node parent) {
		// set defaults
		binary = false;
		// set parent node
		this.parent = parent;
		this.children = null;
	}

	/**
	 * Getter method for parent node
	 * 
	 * @return parent node
	 */
	public Node parent() {
		return parent;
	}

	/**
	 * Getter method for children nodes
	 * 
	 * @return children nodes
	 */
	public List<Node> children() {
		return children;
	}

	/**
	 * Add a child node in standard traversal
	 * 
	 * @param node
	 */
	public void add(Node node) {
		if (standard()) {
			children.add(node);
		}
	}

	/**
	 * Add an array of child nodes in standard traversal
	 * 
	 * @param nodes
	 */
	public void add(Node[] nodes) {
		if (standard()) {
			children.addAll(Arrays.asList(nodes));
		}
	}

	/**
	 * Add an array list of child nodes in standard traversal
	 * 
	 * @param nodes
	 */
	public void add(List<Node> nodes) {
		if (standard()) {
			children.addAll(nodes);
		}
	}

	/**
	 * Getter method for left child in binomial traversal
	 * 
	 * @return
	 */
	public Node left() {
		return binary() ? children.get(0) : null;
	}

	/**
	 * Getter method for right child in binomial traversal
	 * 
	 * @return
	 */
	public Node right() {
		return binary() ? children.get(1) : null;
	}

	/**
	 * Set left node in binomial traversal
	 * 
	 * @param node
	 */
	public void setLeft(Node node) {
		if (binary())
			children.set(0, node);
	}

	/**
	 * Set right node in binomial traversal
	 * 
	 * @param node
	 */
	public void setRight(Node node) {
		if (binary())
			children.set(1, node);
	}

	/**
	 * Initialize as binomial traversal if not already defined
	 * 
	 * @return true if binomial
	 */
	private boolean binary() {
		if (children == null) {
			// initialize as binomial traversal if null
			children = new ArrayList<Node>(2);
			children.add(0, null);
			children.add(1, null);
			binary = true;
			return true;
		} else {
			return binary;
		}
	}

	/**
	 * Initialize as standard traversal if not already defined
	 * 
	 * @return true if standard
	 */
	private boolean standard() {
		if (children == null) {
			// initialize as standard traversal if null
			children = new ArrayList<Node>();
			binary = false;
			return true;
		} else {
			return !binary;
		}
	}
}
