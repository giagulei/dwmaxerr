package gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ZOrderDriver {
	
	// assuming equal size of dimensions
	public static String zSortDataset(String input, long N, int splitSize) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
			String outputFile = input+"_partitioned_"+splitSize;
		
			Path inputPath = new Path(input);
			Path outputPath = new Path("ZSORTOUT");
			Configuration conf = new Configuration();
			conf.set("PSIZE", Integer.toString(splitSize));
			conf.set("OUTFOLDER", outputFile);
			Job job = Job.getInstance(conf, "zorder_sorting");

			job.setMapOutputKeyClass(IntWritable.class); // zorder-index
			job.setMapOutputValueClass(IntWritable.class); // joint-frequency of datacube
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);

			job.setJarByClass(ZOrderDriver.class);
			job.setMapperClass(ZOrderMapper.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);

			job.setReducerClass(ZOrderReducer.class);
			job.setPartitionerClass(ZOrderPartitioner.class);

			FileInputFormat.addInputPath(job, inputPath);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, outputPath);

			//int reducersNum = (int) N/splitSize;
			int reducersNum = 4;
			System.out.println(reducersNum+" REDUCERS");
			job.setNumReduceTasks(reducersNum);
			job.waitForCompletion(true);
			
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputPath, true);
			return outputFile;
	}

	public static void main(String[] args) {

		if( args.length != 3){
			System.out.println("Wrong");
			System.exit(0);
		}

		String input = args[0];
		long N = Long.parseLong(args[1]);
		int splitSize = Integer.parseInt(args[2]);

		
		try {
			zSortDataset(input, N, splitSize);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
