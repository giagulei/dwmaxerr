package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.budgreedy;


import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy.SynopsisMultGreedyAbs;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs.AbstractCombGreedyMapper;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class BudgGreedyMapper extends AbstractCombGreedyMapper<ErrorKeyWritable, NSWCoefficient>{

	private List<Integer> subtreeIDs;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String subtrees = conf.get(WaveletConfProperties.SUBTREE_IDS+"2");
		String [] subtreeRoots = subtrees.split(",");
		subtreeIDs = new ArrayList<>();
		for(int i = 0; i < subtreeRoots.length; i++){
			subtreeIDs.add(Integer.parseInt(subtreeRoots[i]));
		}
		System.out.println(subtrees);
	}
	

	@Override
	public LinkedHashMap<Integer, ErrortreeNodeContents> readLocalSubtree(Context context) throws IOException{
		FileSplit split = (FileSplit)context.getInputSplit();
		Path splitPath = split.getPath();

		Configuration conf = context.getConfiguration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(splitPath));
		HyperNode node = new HyperNode();
		LinkedHashMap<Integer, ErrortreeNodeContents> tree = new LinkedHashMap<Integer, ErrortreeNodeContents>();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			tree.put(hyperNodeIndex, NSWCoefficient.getGreedyTreeNodeProperties(node, hyperNodeIndex, subtreeIDs));
			if(hyperNodeIndex < localRoot) localRoot = hyperNodeIndex;
		}
		reader.close();
		return tree;
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);

		long start = System.currentTimeMillis();

		LinkedHashMap<Integer, ErrortreeNodeContents> tree = readLocalSubtree(context);

		System.out.println("Localroot = "+localRoot);
		System.out.println("No children: "+tree.get(localRoot).getNumOfChilds());

		for(int i = 0; i < DEGREE_OUT; i++){
			if(tree.containsKey(DEGREE_OUT*localRoot+i)){
				localRoot = DEGREE_OUT*localRoot+i;
				break;
			}
		}

		System.out.println(localRoot);
		
		SynopsisMultGreedyAbs greedy = new SynopsisMultGreedyAbs(tree, DEGREE_OUT, localRoot);
		greedy.setContext(context);
		greedy.setB(B);
		greedy.runGreedy();
		
		long end = System.currentTimeMillis();	
		System.out.println("Mapper time: "+(end-start)/1000L);
		cleanup(context);
	}

}



