package decisiontree.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import decisiontree.RecordParser;
import decisiontree.TrainingProgram;

public class TreeMapper extends Mapper<Object, Text, Text, Text> {

	private static ArrayList<String> list = new ArrayList<String>();
	private Text word = new Text();
	private String[] attributeNames;
	private String classifier;
	

	public void setup(Context context) throws IOException,
			InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String attributesString = conf.get("attributeNames");

		RecordParser p = new RecordParser(attributesString);
		// get attribute names from header
		attributeNames = p.values();
		
		// get classifier name from header
		classifier = p.classifier();
		
	}
			


	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		list.add(value.toString());

		if ((list.size() % 100) == 0) {
			
			//System.out.println("we've got us " + list.size() + " records");
			System.out.println("attributes size:" + attributeNames.length);
			System.out.println("class name:" + classifier);
			word.set("we've got us " + list.size() + " records");
			context.write(value, word);
		}

	}
}