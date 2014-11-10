package decisiontree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Attribute implements Serializable {
    private static final Log log = LogFactory.getLog(Attribute.class);
    private String name;
    private Map<String, MutableInt> values;
    
    /**
     * Constructor for attribute
     * @param name
     */
    public Attribute(String name) {
        this.name = name;
        this.values = new HashMap<String, MutableInt>();
    }
    
    /**
     * Constructor for attribute and value
     * @param name
     * @param value 
     */
    public Attribute(String name, String value) {
        this(name);
        add(value);
    }

    /**
     * Constructor for attribute and set of values
     * @param name
     * @param values
     * @param classifier 
     */
    public Attribute(String name, Set<String> values){
        this(name);
        add(values);
    }

    /**
     * Add all values from the given set
     * @param values 
     */
    public void add(Set<String> values){
        for(String value: values) {
            add(value);
        }
    }
    
    /**
     * Add the given value to the set
     * @param value 
     */
    public void add(String value) {
        MutableInt count = values.get(value);
        if(count == null) {
            values.put(value, new MutableInt(1));
        } else {
            count.increment();
        }
    }

    /**
     * Getter for attribute name
     * @return name of the attribute
     */
    public String name() {
        return name;
    }

    /**
     * Getter for the size of the attribute value set
     * @return size of value set
     */
    public int size() {
        return values.size();
    }

    /**
     * Getter for the set of values
     * @return set of values
     */
    public Set<String> values() {
        return values.keySet();
    }
    
    /**
     * Getter for the map of values and counts
     * @return 
     */
    public Map<String, MutableInt> counts() {
        return values;
    }
}
