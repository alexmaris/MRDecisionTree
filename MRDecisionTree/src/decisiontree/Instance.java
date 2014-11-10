package decisiontree;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Instance implements Serializable {
	private static final long serialVersionUID = -8126937682550625335L;
	private static final Log log = LogFactory.getLog(Instance.class);
    private Map<String, String> values;
    private Map<String, Double> valuesDouble;
    private String classifier;

    /**
     * Constructor for instance given an array of names and values
     * @param names
     * @param values
     * @param classifier 
     */
    public Instance(String[] names, String[] values, String classifier){
        this.valuesDouble = new LinkedHashMap<String, Double>();
        this.values = new LinkedHashMap<String, String>();
        this.classifier = classifier;
        for(int i = 0; i < names.length; i++){
            // all attributes available as discrete string values
            this.values.put(names[i], values[i]);
            // marked attributes available as continuous ranged values
            if(isContinuous(names[i])) {
                Double d = 0.0;
                try {
                    d = Double.parseDouble(values[i]);
                } catch (NumberFormatException nfe) {
                    log.info("Number format exception for value " + values[i] + " on attribute " + names[i]);
                }
                this.valuesDouble.put(names[i], d);
            }
        }
    }
    
    /**
     * Getter method for list of attribute names on this instance
     * @return set of attribute names
     */
    public Set<String> attributes(){
        return values.keySet();
    }
    
    /**
     * Getter method for mapped values
     * @return mapped values
     */
    public Map<String, String> values() {
        return values;
    }
    
    /**
     * Getter method for a specific attribute value
     * @param name
     * @return string value
     */
    public String value(String name){
        return values.get(name);
    }
    
    /**
     * Getter method for a specific attribute value
     * @param name
     * @return double value
     */
    public Double valueDouble(String name){
        return valuesDouble.get(name);
    }

    /**
     * Getter method for whether a given attribute contains
     * continuous ranged values
     * @param name
     * @return true if the attribute contains continuous ranged values
     */
    public static boolean isContinuous(String name) {
        return name.startsWith("#", 0);
    }

    /**
     * Getter method for whether a given attribute contains
     * discrete values
     * @param name
     * @return true if the attribute contains discrete values
     */
    public static boolean isDiscrete(String name) {
        return name.startsWith("@") || !name.startsWith("#", 0);
    }

    /**
     * Getter method for the instance classifier
     * @return 
     */
    public String classifier() {
        return classifier;
    }
    
    @Override
    public String toString(){
    	String str = new String();
    	
    	for(String s: attributes()){
    		str += values.get(s)+ ",";
    	}
    	str+= classifier;
    	
    	return str;
    }
    
}
