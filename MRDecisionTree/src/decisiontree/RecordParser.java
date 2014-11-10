package decisiontree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RecordParser {
    private static final Log log = LogFactory.getLog(RecordParser.class);
    private List<String> attributes = null;
    private List<String> values = null;
    private String classifier;
    
    /**
     * Default constructor for parser
     * @param s 
     */
    public RecordParser(String s) {
        // make sure there is data to parse
        if(s == null || s.length() == 0) return;
        // split string and convert to array list
        
        attributes = new ArrayList<String>();
        for (String str : s.split(",")) {
			attributes.add(new String(str).intern());
		}
        //attributes = new ArrayList<String>(Arrays.asList(s.split(",")));
        // extract values from the array list, which excludes the last column
        values = new ArrayList<String>();
        //attributes.subList(0, attributes.size()-1);
        for(String str: attributes.subList(0, attributes.size()-1)){
        	values.add(new String(str).intern());
        }
        
        // extract classifier from the array list, which is the last column
        classifier = new String(attributes.get(attributes.size()-1)).intern();
    }
    
    /**
     * Getter method for array of values
     * @return array of values
     */
    public String[] values() {
        return values != null ? values.toArray(new String[0]) : null;
    }
    
    /**
     * Getter method for array of values typed double
     * Note - use this method only if you know all of your values are
     * continuous ranged values, i.e. real numbers
     * @return 
     */
    public Double[] doubles() {
        List<Double> doubles = new ArrayList<Double>(values.size());
        for(String value: values) {
            try {
                Double d = Double.parseDouble(value);
                doubles.add(d);
            } catch (NumberFormatException nfe) {
                log.info("Number format exception for value " + value);
                doubles.add(0.0);
            }
        }
        return doubles.toArray(new Double[0]);
    }
    
    /**
     * Getter method for classifier
     * @return classifier string
     */
    public String classifier() {
        return values != null ? classifier : null;
    }
}
