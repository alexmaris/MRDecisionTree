package decisiontree.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import decisiontree.Id3;

public class TreeReducer extends Reducer<Text, Id3, Text, Text> {

		private Text classification;
		// Print the results of the mapper
		public void reduce(Text key, Iterable<Id3> values, Context context) 
				throws IOException, InterruptedException {
			
			//classification = values.iterator().next();
			int i = 0;
			
			
			while(values.iterator().hasNext()){
				i++;
				values.iterator().next();
			}
			classification = new Text("Number of trees: " + Integer.toString(i));

			context.write(key, classification);
		}

}
