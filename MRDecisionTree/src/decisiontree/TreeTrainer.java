package decisiontree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TreeTrainer {
	private static final Log log = LogFactory.getLog(TreeTrainer.class);

	private Instances instances;

	/**
	 * Default constructor
	 * 
	 * @param instances
	 */
	public TreeTrainer(Instances instances) {
		this.instances = instances;
	}

	/**
	 * Split a collection of Instances into a new collection with a smaller set
	 * of attributes that are picked at random
	 * 
	 * @param instances
	 * @return
	 */
	protected Instances splitInstancesByAttributesRandomly(Instances instances) {
		// retrieve list of attributes for this instance set
		List<String> attributes = new ArrayList<String>(instances.attributes());
		// shuffle the list of attributes
		Collections.shuffle(attributes);
		// compute a count for the filtered attribute set
		int count = (int) Math.round(Math.sqrt(attributes.size()));
		// get hashset of the filtered attributes set
		Set<String> filters = new HashSet<String>(attributes.subList(0, count));
		// retrieve set of instances with this attribute filter set
		return new Instances(instances, filters);
	}

	/**
	 * Create new trees from a random set of attributes
	 * 
	 * @param count
	 *            Number of trees to return
	 * @return Array of trees trained on random attributes
	 */
	public Id3[] getTreesTrainedFromRandomAttributes(int count) {
		Id3[] trees = new Id3[count];

		int instance_size = (int) Math.sqrt(this.instances.attributes().size());
		
		for (int i = 0; i < count; i++) {
			log.info("Creating tree " + i + " from random attributes");
			
			Id3 trainer = new Id3(this.instances);
            // set tree as random forest
            trainer.setRandomForest(instance_size);
            // train the tree
            log.info("Start traversing training tree");
            trainer.traverse();
            // clear training data
            log.info("Clearing training data");
            trainer.clear();
            
			trees[i] = trainer;
		}

		return trees;
	}

	/**
	 * Create a new tree from a random set of attributes
	 */
	public Id3 getTreeTrainedFromRandomAttributes() {

		int instance_size = (int) Math.sqrt(this.instances.attributes().size());
		
		Id3 trainer = new Id3(this.instances);
        // set tree as random forest
        trainer.setRandomForest(instance_size);
        // train the tree
        trainer.traverse();

        // clear training data
        trainer.clear();
        
        return trainer;
	}
}
