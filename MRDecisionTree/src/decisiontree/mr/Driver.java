package decisiontree.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class Driver {
	public void run(String inputPath, String outputPath, String trainingData, int k) throws Exception {
		
		
		Configuration conf = new Configuration();
		//Job job = Job.getInstance(conf);
		Job job = new Job(conf); // amazon does not support getInstance.
		job.setJobName("MRDecissionTree_Bag_Of_Trees");
		job.getConfiguration().set("testData", trainingData);
		//job.getConfiguration().setInt("kValue", k);
		job.getConfiguration().set("attributeNames", "#duration,@protocol_type,@service,@flag,#src_bytes,#dst_bytes,@land,#wrong_fragment,#urgent,#hot,#num_failed_logins,@logged_in,#num_compromised,#root_shell,#su_attempted,#num_root,#num_file_creations,#num_shells,#num_access_files,#num_outbound_cmds,@is_host_login,@is_guest_login,#count,#srv_count,#serror_rate,#srv_serror_rate,#rerror_rate,#srv_rerror_rate,#same_srv_rate,#diff_srv_rate,#srv_diff_host_rate,#dst_host_count,#dst_host_srv_count,#dst_host_same_srv_rate,#dst_host_diff_srv_rate,#dst_host_same_src_port_rate,#dst_host_srv_diff_host_rate,#dst_host_serror_rate,#dst_host_srv_serror_rate,#dst_host_rerror_rate,#dst_host_srv_rerror_rate,class");	 
		job.setJarByClass(Driver.class);
		job.setMapperClass(TreeMapper.class);
		
		/*
		job.setReducerClass(KnnReducer.class);*/
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileInputFormat.setMaxInputSplitSize(job, 262144);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    
	        job.submit();
		 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		// Make sure that an input, output directory as well training data file are provided
		if (args.length < 3) {
		    System.out.println("Usage Parms: <input dir> <output dir> <test data> <K Value (optional)>");
	            System.exit(-1);
		}
		
		int k =1;
		if (args.length == 4) {
			k = Integer.parseInt(args[3]);
		} else{
			System.out.println("K value not specified, defaulting to '1'");
		}

		// Run the MapReduce job
		Driver driver = new Driver();
		driver.run(args[0], args[1], args[2], k);

	}
}
