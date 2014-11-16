package decisiontree.mr;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

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

			BytesWritable bytes = new BytesWritable(bot.serializeBagToBytes());
			
			
			
			String path = context.getConfiguration().get("outputPath");
			
			Path file = new Path(path + "forest.trees");
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			
			context.write(NullWritable.get(), bytes);
			
			// Write data output to file on hdfs
			DataOutputStream dos = fs.create(file);
			dos.write(bot.serializeBagToBytes());
			
		}
		
	    private static String toString( Serializable o ) throws IOException {
	    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
	        ObjectOutputStream os = new ObjectOutputStream(bos);
	        os.writeObject(o);
	        String serializedObject = bos.toString();
	        os.close();
			return serializedObject;
	    }

}
