package decisiontree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ClassificationProgram {
	private static final Log log = LogFactory
			.getLog(ClassificationProgram.class);

	private BagOfTrees botIn;
	private String[] featureNames;
	private List<String> rawData;
	private List<String> classifiedData;
	private String outputFilePath;

	public ClassificationProgram(String dataInputFile, String dataOtuputFile,
			String treeBagFile) {
		log.info("Reading bag of trees from file");
		botIn = new BagOfTrees();
		botIn.readBagFromFile(treeBagFile);

		log.info("Tree bag contains " + botIn.count() + " trees");

		outputFilePath = dataOtuputFile;

		rawData = loadDataFromFile(dataInputFile);
	}

	public void setfeatureNames(String[] names) {
		this.featureNames = names;
	}

	private List<String> loadDataFromFile(String path_to_file) {
		List<String> list = new ArrayList<String>();

		log.info("Loading testing data set");
		String fileRow;
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(path_to_file);
			reader = new BufferedReader(new InputStreamReader(fis));

			// parse records
			while ((fileRow = reader.readLine()) != null) {
				list.add(fileRow);
			}
		} catch (FileNotFoundException e) {
			System.out.println("FileNotFoundException issued");
		} catch (IOException e) {
			System.out.println("IOException issued");
		} finally {
			try {
				if (reader != null)
					reader.close();
				if (fis != null)
					fis.close();
			} catch (IOException e) {
				System.out.println("IOException when closing file");
			}
		}

		log.debug("Loaded " + list.size() + " records");
		return list;
	}

	public void classifyData() {
		classifiedData = new ArrayList<String>(rawData.size());
		RecordParser parser;
		Instance instance;
		String guessedClassification;

		int reportSize = rawData.size() / 10;

		for (int i = 0; i < rawData.size(); i++) {
			if ((i % reportSize) == 0) {
				log.info("Classifying data "
						+ (int) ((double) i / rawData.size() * 100)
						+ "% complete");
			}

			// Parse the row from the raw data, adding a comma where the
			// classification would normally go
			parser = new RecordParser(rawData.get(i) + ", ");

			instance = new Instance(featureNames, parser.values(),
					parser.classifier());

			guessedClassification = botIn.classifyByVote(instance);

			// Add classified instance to the tree
			classifiedData.add(instance.toString() + guessedClassification);
		}
	}

	public void saveClassifiedData() {
		log.info("Saving classified data out to file: " + outputFilePath);

		FileWriter writer;
		try {
			writer = new FileWriter(outputFilePath);

			for (String str : classifiedData) {
				writer.write(String.format("%s%n", str));
			}
			writer.close();
		} catch (IOException e) {
			log.error("IOException issued");
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		String inputFile = "data/kddcup.testdata.unlabeled.txt";
		String otuputFile = "data/classified_data.txt";
		String treeFile = "data/kddcup.trees";

		// Create some test instances that we can try and classify
		String[] names = { "#duration", "@protocol_type", "@service", "@flag",
				"#src_bytes", "#dst_bytes", "@land", "#wrong_fragment",
				"#urgent", "#hot", "#num_failed_logins", "@logged_in",
				"#num_compromised", "#root_shell", "#su_attempted",
				"#num_root", "#num_file_creations", "#num_shells",
				"#num_access_files", "#num_outbound_cmds", "@is_host_login",
				"@is_guest_login", "#count", "#srv_count", "#serror_rate",
				"#srv_serror_rate", "#rerror_rate", "#srv_rerror_rate",
				"#same_srv_rate", "#diff_srv_rate", "#srv_diff_host_rate",
				"#dst_host_count", "#dst_host_srv_count",
				"#dst_host_same_srv_rate", "#dst_host_diff_srv_rate",
				"#dst_host_same_src_port_rate", "#dst_host_srv_diff_host_rate",
				"#dst_host_serror_rate", "#dst_host_srv_serror_rate",
				"#dst_host_rerror_rate", "#dst_host_srv_rerror_rate" };

		ClassificationProgram classifier = new ClassificationProgram(inputFile,
				otuputFile, treeFile);

		classifier.setfeatureNames(names);
		classifier.classifyData();
		classifier.saveClassifiedData();

		System.out.println("END program.");

		/*
		 * String[] valuesSmurf = { "0", "icmp", "ecr_i", "SF", "1032", "0",
		 * "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		 * "0", "0", "511", "511", "0.00", "0.00", "0.00", "0.00", "1.00",
		 * "0.00", "0.00", "255", "255", "1.00", "0.00", "1.00", "0.00", "0.00",
		 * "0.00", "0.00", "0.00" }; String[] valuesNormal = { "0", "tcp",
		 * "http", "SF", "336", "3841", "0", "0", "0", "0", "0", "1", "0", "0",
		 * "0", "0", "0", "0", "0", "0", "0", "0", "7", "11", "0.00", "0.00",
		 * "0.00", "0.00", "1.00", "0.00", "0.27", "33", "255", "1.00", "0.00",
		 * "0.03", "0.07", "0.00", "0.00", "0.00", "0.00" };
		 * 
		 * Instance instanceToClasify = new Instance(names, valuesSmurf, null);
		 * Instance instanceToClasify2 = new Instance(names, valuesNormal,
		 * null);
		 * 
		 * // Pull out one of the trees from the bag and try to classify our
		 * test // records
		 * System.out.println(botIn.classifyByVote(instanceToClasify));
		 * System.out.println(botIn.classifyByVote(instanceToClasify2));
		 */
	}
}
