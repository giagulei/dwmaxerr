package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.budgreedy;


import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
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


public class BudgGreedyDriver {

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

	public static double createSynopsis(int B, int dim, String synopsisPath) throws IOException, 
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
		String subtreeIDs2 = "";
		for(Integer rootLeaf:rootTreeLeafsIndex){
			System.out.println("Examined rootLeaf: "+rootLeaf);
			if(rootTreeLeafsIndex.contains(degree_out*rootLeaf)) continue;
			System.out.println("Its a leaf!");
			subtreeIDs2 += rootLeaf + ",";
			int factor = rootLeaf * degree_out;
			for(int j = 0; j < degree_out; j++){
				System.out.println("Also adding: "+(factor+j));
				subtreeIDs += (factor + j) + ",";
			}
		}

		System.out.println("Subtrees: "+subtreeIDs);
		System.out.println("Subtrees2: "+subtreeIDs2);

		conf.setInt(WaveletConfProperties.ROOT_TREE_SIZE, rootTreeLeafsIndex.size());
		conf.set(WaveletConfProperties.SUBTREE_IDS, subtreeIDs);
		conf.set(WaveletConfProperties.SUBTREE_IDS+"2", subtreeIDs2);

		conf.setInt(WaveletConfProperties.DIM, dim);
		conf.setInt(WaveletConfProperties.BUDGET, B);

		Path inputPath = new Path(NSWDecomposition.BASE_SUBTREES_PATH);

		Job job = Job.getInstance(conf, "simple_synopsis_driver");
		job.addCacheFile(new URI(rootTreePath));
		job.setMapOutputKeyClass(ErrorKeyWritable.class);
		job.setMapOutputValueClass(NSWCoefficient.class);

		job.setOutputKeyClass(NSWCoefficient.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(BudgGreedyDriver.class);
		job.setReducerClass(BudgGreedyReducer.class);
		job.setMapperClass(BudgGreedyMapper.class);
		//job.setCombinerClass(WaveletMergeCombiner.class);
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
		Integer dim = Integer.parseInt(args[1]);
		String synopsisPath = args[2];

		System.out.println("B = "+B);
		System.out.println("dim = "+dim);

		createSynopsis(B, dim, synopsisPath);
	}

}
