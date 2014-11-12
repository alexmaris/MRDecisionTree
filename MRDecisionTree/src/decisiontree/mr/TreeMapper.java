package decisiontree.mr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import decisiontree.Id3;
import decisiontree.Instance;
import decisiontree.Instances;
import decisiontree.RecordParser;

public class TreeMapper extends Mapper<Object, Text, NullWritable, Id3> {

	private String[] attributeNames;
	private String classifier;

	private Instance instance;

	private ArrayList<Instance> instanceList;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		instanceList = new ArrayList<Instance>();

		Configuration conf = context.getConfiguration();
		String attributesString = conf.get("attributeNames");

		RecordParser p = new RecordParser(attributesString);
		// get attribute names from header
		attributeNames = p.values();

		// get classifier name from header
		classifier = p.classifier();
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			instance = parseStringToInstance(value.toString());
			instanceList.add(instance);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private Instance parseStringToInstance(String string) {
		// parse data record
		RecordParser parser = new RecordParser(string);

		// add the instance attribute names and values
		return new Instance(attributeNames, parser.values(),
				parser.classifier());

	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		Instances instances = new Instances(instanceList);

		int instance_size = (int) Math.sqrt(instances.attributes().size());

		for (int i = 0; i < 1; i++) {

			Id3 trainer = new Id3(instances);
			// set tree as random forest
			trainer.setRandomForest(instance_size);
			// train the tree
			trainer.traverse();

			// clear training data
			trainer.clear();

			context.write(NullWritable.get(), trainer);
		}
	}
}