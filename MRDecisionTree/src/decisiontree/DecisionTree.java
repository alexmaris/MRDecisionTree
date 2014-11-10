package decisiontree;

import java.io.File;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DecisionTree {
    private static final Log log = LogFactory.getLog(DecisionTree.class);
    // size of cross validation
    private static int CROSS_VALIDATION_SIZE = 10;
    // internal constant for data set file path
    private static String PATH_TO_FILE = "data/iris.data";
    // attributes derived from instances
    private Map<String, Attribute> attributes;
    // instances derived from data set
    private Instances instances;
    // cross validation instances
    private Instances[] split;
    // cross validation trainers
    private Id3[] trainer;
    // trainer with highest accuracy
    int selected;
    
    /**
     * Constructor for decision tree with a given data set
     * @param file 
     */
    public DecisionTree(String file){
        /**
         * Load instances from file
         */
        log.info("Loading file " + file);
        instances = new Instances(new File(file));
        if(instances.size() == 0) {
            log.warn("Instances set is null");
        }
        log.info("Loaded " + instances.size() + " instances");
    }
    
    /**
     * Constructor for decision tree with given instances
     * @param instances
     */
    public DecisionTree(Instances instances){
    	this.instances = instances;
    }
    
    
    public void crossValidation() {
        // split instances for cross validation tests
        split = instances.split(CROSS_VALIDATION_SIZE);
        // allocate training trees for cross validation
        trainer = new Id3[CROSS_VALIDATION_SIZE];
        // train and test each tree
        for(int i = 0; i < CROSS_VALIDATION_SIZE; i++) {
            log.info("Starting cross validation split " + i);
            Instances trainedInstances = Instances.merge(split, i);
            // instantiate training tree
            trainer[i] = new Id3(trainedInstances);
            // traverse the tree from root node
            log.info("Starting training split " + i);
            trainer[i].traverse();
            // prune the tree
            log.info("Starting pruning split " + i);
            trainer[i].prune();
            // classify test instances
            log.info("Starting testing split " + i);
            trainer[i].test(split[i]);
            log.info("Split " + i + " resulted in accuracy " + trainer[i].accuracy());
        }
        // find highest accuracy trainer
        selected = 0;
        double accuracy = 0;
        for(int i = 0; i < CROSS_VALIDATION_SIZE; i++) {
            if(trainer[i].accuracy() > accuracy) {
                accuracy = trainer[i].accuracy();
                selected = i;
            }
        }
    }
    
    public Id3 getBestTrainter(){
    	return trainer[selected];
    }
    
    /**
     * Print results
     */
    public void print() {
        // print confusion matrix for the selected trainer set
        trainer[selected].printConfusionMatrix();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        log.info("Starting DecisionTree execution");

        /**
         * Instantiate decision tree with static data set
         */
        DecisionTree dt = new DecisionTree(PATH_TO_FILE);
        
        /**
         * Perform cross validation tests
         */
        dt.crossValidation();

        /**
         * Print results
         */
        dt.print();
    }
    
}
