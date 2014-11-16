package decisiontree.dataprep;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import decisiontree.ClassificationProgram;

public class DataPrepper {
	private static final Log log = LogFactory.getLog(DataPrepper.class);
	
	private static List<String> fileData = new ArrayList<String>(100000);
	
	public static void main(String[] args) {
		
		String inputFile = "data/kddcup.data.txt";
		String outputFile = "data/kddcup.randomized.data.txt";

		List<String> fileData = new ArrayList<String>(100000);

		// Load data into memory
		loadData(inputFile);

		// Randomize the data list
		Collections.shuffle(fileData);

		// Write data to file
		writeData(outputFile);

	}

	public static void loadData(String path_to_file) {
		log.info("Loading data to memory");
		String fileRow;
		FileInputStream fis = null;
		BufferedReader reader = null;
		try {
			fis = new FileInputStream(path_to_file);
			reader = new BufferedReader(new InputStreamReader(fis));

			// parse records
			while ((fileRow = reader.readLine()) != null) {
				fileData.add(fileRow);
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

		log.debug("Loaded " + fileData.size() + " records");
	}
	
	public static void writeData(String path_to_file){
		log.info("Saving preped data out to file: " + path_to_file);

		FileWriter writer;
		try {
			writer = new FileWriter(path_to_file);

			for (String str : fileData) {
				writer.write(str);
			}
			writer.close();
		} catch (IOException e) {
			log.error("IOException issued");
			e.printStackTrace();
		}
	}
}
