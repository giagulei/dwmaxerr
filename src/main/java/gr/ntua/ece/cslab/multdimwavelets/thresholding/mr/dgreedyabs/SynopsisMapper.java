package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs;


import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy.SynopsisMultGreedyAbs;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;

public class SynopsisMapper extends AbstractCombGreedyMapper<ErrorKeyWritable, NSWCoefficient>{

	private BitSet bestCombination;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String bComb = conf.get(WaveletConfProperties.BEST_COMB);
		bestCombination = SynopsisGreedyRootDriver.getBitsetFromString(bComb);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);

		long start = System.currentTimeMillis();

		LinkedHashMap<Integer, ErrortreeNodeContents> tree = readLocalSubtree(context);
		double incomingError = computeIncomingError(localRoot, bestCombination, rootTreeMap);		
		updateTreeNodes(incomingError, tree);
		
		SynopsisMultGreedyAbs greedy = new SynopsisMultGreedyAbs(tree, DEGREE_OUT, localRoot);
		greedy.setB(B);
		greedy.setContext(context);
		greedy.runGreedy();
		
		long end = System.currentTimeMillis();
		System.out.println("Mapper time: "+(end-start)/1000L);
		cleanup(context);
	}

}



