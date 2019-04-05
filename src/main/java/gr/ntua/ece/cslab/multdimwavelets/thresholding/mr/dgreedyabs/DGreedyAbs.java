package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy.BaseMultGreedyAbs;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorBySubtreeWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.MDErrorContainer;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.WaveletCoefArrayWritable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;


public class DGreedyAbs{

	public static final String LOW_OUT = "LOW_OUT";
	public static final String HIGH_OUT = "HIGH_OUT";
	public static final String SYNOPSIS_NAME = "SYNOPSIS";
	
	public static double kSIMILAR = 100;

	public static ArrayList<BitSet> getAndPersistRootCombinations(List<MDErrorContainer> rootSynopsis, int degreeOut,
			Configuration conf){

		ArrayList<BitSet> retainedCombinations  = new ArrayList<BitSet>();

		Set<Integer> retainedHyperNodeIndices = new HashSet<Integer>();
		ArrayList<String> retainedCoefs = new ArrayList<String>();

		BitSet combination = new BitSet();
		// adding the empty combination -- no node is retained from root subtree
		//retainedCombinations.add(combination);
		for(int i = rootSynopsis.size()-1; i >= 0; i--){
			retainedCoefs.add(rootSynopsis.get(i).getNodeIndex());
			String[] indices = rootSynopsis.get(i).getNodeIndex().split("_");
			int hyperNodeIndex = Integer.parseInt(indices[0]);
			int internalNodeIndex = Integer.parseInt(indices[1]);
			int nodeIndex =  hyperNodeIndex * degreeOut + internalNodeIndex;

			retainedHyperNodeIndices.add(nodeIndex);
			combination = new BitSet();
			for(Integer index:retainedHyperNodeIndices){
				combination.set(index);
			}
			retainedCombinations.add(combination);

		}
		return retainedCombinations;
	}

	public static double run(String splitSize, String dimension, String budget) throws Exception{
		//long start = System.currentTimeMillis();
		int dim = Integer.parseInt(dimension);
		int degreeOut = (int)Math.pow(2, dim);
		NSWCoefficient.initializeCoefs(dim);

		int B = Integer.parseInt(budget);

		Configuration conf = new Configuration();

		//put root tree to cache
		String rootTreePath = NSWDecomposition.ERRORTREE+"0";

		//load root subtree from hdfs to memory 
		Path rootTree = new Path(rootTreePath);
		HashMap<Integer, ErrortreeNodeContents> rootTreeMap = new HashMap<Integer, ErrortreeNodeContents>();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(rootTree));
		HyperNode node = new HyperNode();
		List<Integer> rootTreeLeafsIndex = new ArrayList<Integer>();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			rootTreeLeafsIndex.add(hyperNodeIndex);
			rootTreeMap.put(hyperNodeIndex, NSWCoefficient.getGreedyTreeNodeProperties(node, hyperNodeIndex));
			// print root subtree
//			for(NSWCoefficient c : rootTreeMap.get(hyperNodeIndex).getCoefficientNodes()){
//				System.out.println(c.toString());
//			}
		}
		reader.close();

		//run greedy stat and keep resulted combinations
		BaseMultGreedyAbs<ErrorBySubtreeWritable, WaveletCoefArrayWritable> greedy
		= new BaseMultGreedyAbs<ErrorBySubtreeWritable, WaveletCoefArrayWritable>(rootTreeMap, degreeOut, false);
		greedy.setB((rootTreeMap.size()-1)*(degreeOut-1)+1); // i need all the root tree, i.e., the coefs and not the hypernodes
		greedy.runGreedy();
		List<MDErrorContainer> rootTreeGreedyResult = greedy.getSynopsis();

		ArrayList<BitSet> rootCombs = getAndPersistRootCombinations(rootTreeGreedyResult, degreeOut, conf);

		//System.out.println(rootCombs.toString());
		
		FileSystem fs = FileSystem.get(conf);
				
		int lowerBoundIndex = rootCombs.size()-1;
		double lowerBoundError = 0;
		
		int upperBoundIndex = 0;
		double upperBoundError = 0;
		
		//System.out.println("Lower Bound for comb: "+rootCombs.get(lowerBoundIndex).toString());
		lowerBoundError =  SynopsisGreedyMergeDriver.createSynopsis(B, rootCombs.get(lowerBoundIndex).toString(), dim, LOW_OUT);
		//fs.rename(new Path(GREEDY_OUTPUT), new Path(SYNOPSIS_NAME));
		
		//System.out.println("Upper Bound for comb: "+rootCombs.get(upperBoundIndex).toString());
		upperBoundError = SynopsisGreedyMergeDriver.createSynopsis(B, rootCombs.get(upperBoundIndex).toString(), dim, HIGH_OUT);
		
		
		while(Math.abs(upperBoundError - lowerBoundError) >= kSIMILAR){		
			if(upperBoundError > lowerBoundError){
				fs.delete(new Path(HIGH_OUT), true);
				upperBoundIndex ++;
				//System.out.println("Upper Bound for comb: "+rootCombs.get(upperBoundIndex).toString());
				upperBoundError = SynopsisGreedyMergeDriver.createSynopsis(B, rootCombs.get(upperBoundIndex).toString(), dim, HIGH_OUT);
			}else{
				fs.delete(new Path(LOW_OUT), true);
				lowerBoundIndex--;
				//System.out.println("Lower Bound for comb: "+rootCombs.get(lowerBoundIndex).toString());
				lowerBoundError =  SynopsisGreedyMergeDriver.createSynopsis(B, rootCombs.get(lowerBoundIndex).toString(), dim, LOW_OUT);
			}
		}
		
		double min = 0;
		if(upperBoundError > lowerBoundError){
			min = lowerBoundError;
			fs.rename(new Path(LOW_OUT), new Path(SYNOPSIS_NAME));
			fs.delete(new Path(HIGH_OUT), true);
		}else{
			min = upperBoundError;
			fs.rename(new Path(HIGH_OUT), new Path(SYNOPSIS_NAME));
			fs.delete(new Path(LOW_OUT), true);
		}
		
		System.out.println("minimum error = "+min);
		return min;
	}

	public static void main(String[] args) throws Exception {

		if(args.length != 3){
			System.out.println("Arguments: splitsize, budget, dim");
			System.exit(0);
		}
		String splitSize = args[0];
		run(splitSize, args[1], args[2]);
	}

}
