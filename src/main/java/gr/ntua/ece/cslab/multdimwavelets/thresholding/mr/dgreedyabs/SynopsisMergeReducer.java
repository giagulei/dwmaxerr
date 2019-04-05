package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.WaveletMergeReducer;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;


public class SynopsisMergeReducer extends WaveletMergeReducer {


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// read root-subtree
		Configuration conf = context.getConfiguration();

		int dim = Integer.parseInt(conf.get(WaveletConfProperties.DIM));
		int degOut = (int) Math.pow(2, dim);

		URI[] cacheFiles = context.getCacheFiles();

		if(result){
		HashMap<Integer, HyperNode> rootTreeMap = new HashMap<Integer, HyperNode>();
		Path rootErrorTreePath = new Path(cacheFiles[0]);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(rootErrorTreePath));
		HyperNode node = new HyperNode();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			rootTreeMap.put(hyperNodeIndex, node.clone());
		}
		reader.close();	

		String bComb = conf.get(WaveletConfProperties.BEST_COMB);
		BitSet bestCombination = SynopsisGreedyRootDriver.getBitsetFromString(bComb);

		ArrayList<Integer> retainedIndices = new ArrayList<Integer>();
		for (int i = bestCombination.nextSetBit(0); i != -1; i = bestCombination.nextSetBit(i + 1)) {
			retainedIndices.add(i);
		}
		for(Integer nodeIndex: retainedIndices){
			int hyperNodeIndex = nodeIndex / degOut;
			int internalNodeIndex = nodeIndex % degOut;
			NSWCoefficient retainedCoefficient = rootTreeMap.get(hyperNodeIndex).getCoefficientNodes().get(internalNodeIndex);
			synopsis.add(retainedCoefficient);
			count++;
		}
		}
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		
	}

}
