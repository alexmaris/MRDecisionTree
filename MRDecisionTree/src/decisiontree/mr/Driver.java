package decisiontree.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import decisiontree.Id3;

public class Driver {
	public void run(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();

		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");

		// Job job = Job.getInstance(conf);
		Job job = new Job(conf); // amazon does not support getInstance.
		job.setJobName("MRDecissionTree_Bag_Of_Trees");
		job.setJarByClass(Driver.class);

		job.getConfiguration()
				.set("attributeNames",
						"#duration,@protocol_type,@service,@flag,#src_bytes,#dst_bytes,@land,#wrong_fragment,#urgent,#hot,#num_failed_logins,@logged_in,#num_compromised,#root_shell,#su_attempted,#num_root,#num_file_creations,#num_shells,#num_access_files,#num_outbound_cmds,@is_host_login,@is_guest_login,#count,#srv_count,#serror_rate,#srv_serror_rate,#rerror_rate,#srv_rerror_rate,#same_srv_rate,#diff_srv_rate,#srv_diff_host_rate,#dst_host_count,#dst_host_srv_count,#dst_host_same_srv_rate,#dst_host_diff_srv_rate,#dst_host_same_src_port_rate,#dst_host_srv_diff_host_rate,#dst_host_serror_rate,#dst_host_srv_serror_rate,#dst_host_rerror_rate,#dst_host_srv_rerror_rate,class");

		job.getConfiguration().set("outputPath", outputPath);
		
		job.setMapperClass(TreeMapper.class);
		job.setReducerClass(TreeReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Id3.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesValueOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileInputFormat.setMaxInputSplitSize(job, 10000000);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.submit();

		boolean jobCompletion = job.waitForCompletion(true);

		if (jobCompletion) {
			// Read the output and serialize as BagOfTrees

		}

		System.exit((jobCompletion) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		// Make sure that an input, output directory as well training data file
		// are provided
		if (args.length != 2) {
			System.out.println("Usage Parms: <input dir> <output dir>");
			System.exit(-1);
		}

		// Run the MapReduce job
		Driver driver = new Driver();
		driver.run(args[0], args[1]);

	}
}
