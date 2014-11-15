package decisiontree.mr;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import decisiontree.BagOfTrees;
import decisiontree.Id3;

public class TreeReducer extends Reducer<NullWritable, Id3, NullWritable, BytesWritable> {

		private Text classification;
		
		@Override
		public void reduce(NullWritable key, Iterable<Id3> values, Context context) 
				throws IOException, InterruptedException {
			BagOfTrees bot = new BagOfTrees();
			int i = 0;

			while(values.iterator().hasNext()){
				i++;
				Id3 tree =  values.iterator().next();
				
				bot.addTree(tree);
			}
			classification = new Text("Number of trees: " + Integer.toString(i));

			BytesWritable bytes = new BytesWritable(bot.serializeBagToBytes());
			//BytesWritable bytes = new BytesWritable("test".getBytes());
			//context.write(NullWritable.get(), bytes);
			//context.write(NullWritable.get(), classification);
			
			
			Path file = new Path("hdfs://192.168.1.87:8020/ddata/output/forest.trees");
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			SequenceFile.Writer inputWriter = new SequenceFile.Writer(fs, context.getConfiguration(), file, NullWritable.class, BytesWritable.class);
			inputWriter.append(NullWritable.get(), bytes);
			inputWriter.close();
			
		}

}
