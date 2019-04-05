package gr.ntua.ece.cslab.multdimwavelets.nonstandard.mr;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.ReducerDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NonStandardReducer extends
Reducer<IntWritable, NSWCoefficient, NullWritable, NullWritable> {

	private ReducerDecomposition cube;
	private int partitionSize;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		int N = Integer.parseInt(conf.get(WaveletConfProperties.N));
		partitionSize = Integer.parseInt(conf.get(WaveletConfProperties.SPLIT_SIZE));
		int dimensionality = Integer.parseInt(conf.get(WaveletConfProperties.DIM));

		cube = new ReducerDecomposition(N/partitionSize, dimensionality);
	}

	public void reduce(IntWritable key, Iterable<NSWCoefficient> values, Context context)
			throws IOException, InterruptedException {
		
		List<NSWCoefficient> groupedTogether = new ArrayList<NSWCoefficient>();
		Iterator<NSWCoefficient> iter = values.iterator();
		while(iter.hasNext()){
			groupedTogether.add(iter.next().clone());
		}
		Collections.sort(groupedTogether);
		for(int i = 0; i < groupedTogether.size(); i++){
			cube.addTuple(groupedTogether.get(i).getValue());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {	
		cube.transform();
		cube.persistSubTree(new Path(NSWDecomposition.ERRORTREE+"0"));
		super.cleanup(context);
	}

}
