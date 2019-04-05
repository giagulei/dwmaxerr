package gr.ntua.ece.cslab.multdimwavelets.nonstandard.mr;

import java.io.IOException;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning.ZOrderDriver;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class NonStandardDriver {

	public static final String JOB_OUT = "NSW_OUT";

	// assuming equal size of dimensions

	public static void transformDataset(String input, int N, int splitSize, int datasetDim) 
			throws IOException, ClassNotFoundException, InterruptedException{


		Path inputPath = new Path(input);
		Path outputPath = new Path(JOB_OUT);
		Configuration conf = new Configuration();
		conf.set(WaveletConfProperties.N, Integer.toString(N));
		conf.set(WaveletConfProperties.SPLIT_SIZE, Integer.toString(splitSize));
		conf.set(WaveletConfProperties.DIM, Integer.toString(datasetDim));
		
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path(NSWDecomposition.ERRORTREE));
		fs.mkdirs(new Path(NSWDecomposition.BASE_SUBTREES_PATH));
		Job job = Job.getInstance(conf, "nswavelet_decomposition");

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NSWCoefficient.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(NonStandardDriver.class);
		job.setMapperClass(NonStandardMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setReducerClass(NonStandardReducer.class);

		FileInputFormat.addInputPath(job, inputPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
		fs.delete(outputPath, true);
	}

	public static void main(String[] args) {

		if( args.length < 4){
			System.out.println("Wrong");
			System.exit(0);
		}

		String input = args[0];
		int N = Integer.parseInt(args[1]);
		int splitSize = Integer.parseInt(args[2]);
		int datasetDim = Integer.parseInt(args[3]);

		try {
			long start;
			if(datasetDim > 1){
				start = System.currentTimeMillis();
				input = ZOrderDriver.zSortDataset(input, N, splitSize);
				System.out.println("ZSorting Time: "+(System.currentTimeMillis()-start)/1000L);
			}else{
				// partition data				
				Configuration conf = new Configuration();
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path(input)));
				input = input+"_partitioned_"+splitSize;
				Path outputPath = new Path(input);
				SequenceFile.Writer writer = null;
				IntWritable key = new IntWritable();
				IntWritable val = new IntWritable();
				while (reader.next(key, val)) {
					if(key.get() % splitSize == 0){
						if(writer != null) writer.close();
						writer = SequenceFile.createWriter(conf, Writer.file(new Path(outputPath+"/"+key.get())),
								Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));
					}
					writer.append(key, val);
				}
				if(writer != null) writer.close();
				reader.close();
			}
			start = System.currentTimeMillis();
			transformDataset(input, N, splitSize, datasetDim);
			System.out.println("NSWD Time: "+(System.currentTimeMillis()-start)/1000L);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
