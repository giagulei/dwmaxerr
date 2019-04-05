package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs;


import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.WaveletMergeCombiner;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class SynopsisGreedyRootDriver{

	public static BitSet getBitsetFromString(String s){	
		BitSet b = new BitSet();
		if(s.length()>2){
			s = s.substring(1, s.length()-1);
			String[] indices = s.split(",");
			for(String index:indices){
				index = index.replace(" ", "");
				b.set(Integer.parseInt(index));
			}
		}
		return b;
	}

	public static double createSynopsis(int B, String bestCombination, int dim, String synopsisPath) throws IOException, 
	URISyntaxException, ClassNotFoundException, InterruptedException{
		long start = System.currentTimeMillis();

		Configuration conf = new Configuration();

		String rootTreePath = NSWDecomposition.ERRORTREE+"0";

		// find maximum root tree node
		Path rootTree = new Path(rootTreePath);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(rootTree));
		HyperNode node = new HyperNode();
		List<Integer> rootTreeLeafsIndex = new ArrayList<Integer>();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			rootTreeLeafsIndex.add(hyperNodeIndex);
		}
		reader.close();

		int degree_out = (int) Math.pow(2, dim);
		String subtreeIDs = "";
		for(Integer rootLeaf:rootTreeLeafsIndex){
			if(rootTreeLeafsIndex.contains(degree_out*rootLeaf)) continue; // an uparxei sto dentro to *degree_out den eimai fullo
			int factor = rootLeaf * degree_out;
			for(int j = 0; j < degree_out; j++){
				subtreeIDs += (factor + j) + ",";
			}
		}

		//System.out.println("Subtrees: "+subtreeIDs);

		conf.set(WaveletConfProperties.SUBTREE_IDS, subtreeIDs);

		conf.set(WaveletConfProperties.BEST_COMB, bestCombination);
		conf.setInt(WaveletConfProperties.DIM, dim);
		conf.setInt(WaveletConfProperties.BUDGET, B);

		Path inputPath = new Path(NSWDecomposition.BASE_SUBTREES_PATH);

		Job job = Job.getInstance(conf, "dgreedyabs_synopsis_driver");
		job.addCacheFile(new URI(rootTreePath));
		job.setMapOutputKeyClass(ErrorKeyWritable.class);
		job.setMapOutputValueClass(NSWCoefficient.class);

		job.setOutputKeyClass(NSWCoefficient.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(SynopsisGreedyRootDriver.class);
		job.setReducerClass(SynopsisMergeReducer.class);
		job.setMapperClass(SynopsisMapper.class);
		job.setCombinerClass(WaveletMergeCombiner.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);

		FileOutputFormat.setOutputPath(job,new Path(synopsisPath));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);	

		long end = System.currentTimeMillis();
		System.out.println("Execution Synopsis Time = "+(end-start)/1000L+" sec");		

		SequenceFile.Reader seqreader = new SequenceFile.Reader(conf, Reader.file(new Path("FINAL_ERROR")));
		DoubleWritable error = new DoubleWritable();
		while(seqreader.next(error, NullWritable.get())){
			System.out.println("Maximum Error: "+error.get());
		}
		seqreader.close();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("FINAL_ERROR"), true);
		return error.get();
	}

	public static void main(String[] args) throws Exception {

		int B = Integer.parseInt(args[0]);
		String bestCombination = args[1];
		Integer dim = Integer.parseInt(args[2]);
		String synopsisPath = args[3];

		System.out.println("B = "+B);
		System.out.println("best combination = "+bestCombination);
		System.out.println("dim = "+dim);

		createSynopsis(B, bestCombination, dim, synopsisPath);
	}

}
