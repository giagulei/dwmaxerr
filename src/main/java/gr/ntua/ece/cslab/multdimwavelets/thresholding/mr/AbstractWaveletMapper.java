package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AbstractWaveletMapper<X, Y, T, Z> extends Mapper<X, Y, T, Z>{
	
	protected HashMap<Integer, HyperNode> rootTreeMap;
	protected List<BitSet> selectedCombinations;
	protected int DEGREE_OUT;
	protected int localRoot = Integer.MAX_VALUE; // initialized to infinity
	protected int B;
	
	public LinkedHashMap<Integer, ErrortreeNodeContents> readLocalSubtree(Context context) throws IOException{
		FileSplit split = (FileSplit)context.getInputSplit();
		Path splitPath = split.getPath();
		
		Configuration conf = context.getConfiguration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(splitPath));		
		HyperNode node = new HyperNode();
		LinkedHashMap<Integer, ErrortreeNodeContents> tree = new LinkedHashMap<Integer, ErrortreeNodeContents>();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			tree.put(hyperNodeIndex, NSWCoefficient.getGreedyTreeNodeProperties(node, hyperNodeIndex));
			if(hyperNodeIndex < localRoot) localRoot = hyperNodeIndex;
		}
		reader.close();
		return tree;
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = context.getCacheFiles();
		B = Integer.parseInt(conf.get(WaveletConfProperties.BUDGET));
		int dim = Integer.parseInt(conf.get(WaveletConfProperties.DIM));
		NSWCoefficient.initializeCoefs(dim);
		DEGREE_OUT = (int) Math.pow(2, dim);

		rootTreeMap = new HashMap<Integer, HyperNode>();
		Path rootErrorTreePath = new Path(cacheFiles[0]);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(rootErrorTreePath));
		HyperNode node = new HyperNode();
		while(reader.next(node, NullWritable.get())){
			int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
			rootTreeMap.put(hyperNodeIndex, node.clone());
		}
		reader.close();	
	}


}
