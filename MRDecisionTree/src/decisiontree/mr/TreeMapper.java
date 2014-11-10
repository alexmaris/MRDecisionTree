package decisiontree.mr;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TreeMapper extends Mapper<Object, Text, Text, Text> {

	private final static ArrayList<String> list = new ArrayList<String>();
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		list.add(value.toString());

		if ((list.size() % 100) == 0) {
			//System.out.println("we've got us " + list.size() + " records");
			word.set("we've got us " + list.size() + " records");
			context.write(value, word);
		}

	}
}