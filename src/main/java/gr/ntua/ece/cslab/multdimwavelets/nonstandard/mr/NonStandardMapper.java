package gr.ntua.ece.cslab.multdimwavelets.nonstandard.mr;

import java.io.IOException;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWDecomposition;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NonStandardMapper extends Mapper<IntWritable, IntWritable, IntWritable, NSWCoefficient>  {

	private NSWDecomposition cube;
	private int partitionSize;

	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		int N = Integer.parseInt(conf.get(WaveletConfProperties.N));
		partitionSize = Integer.parseInt(conf.get(WaveletConfProperties.SPLIT_SIZE));
		int dimensionality = Integer.parseInt(conf.get(WaveletConfProperties.DIM));
		cube = new NSWDecomposition(partitionSize, dimensionality);
		cube.setN(N);
	}

	@Override
	public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
		cube.addTuple(value.get());
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		if(context.nextKeyValue()){
			int blockID = context.getCurrentKey().get() / partitionSize;
			cube.setBlockID(blockID);
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {	
		cube.transform();
		cube.persistSubTree(new Path(NSWDecomposition.BASE_SUBTREES_PATH+cube.getBlockID()));
		NSWCoefficient localRootAverageCoef = new NSWCoefficient(cube.getLocalRootIndex());
		localRootAverageCoef.setValue(cube.getTopLevelAverage());
		context.write(new IntWritable(cube.getLocalRootIndex()/cube.getSBB()), localRootAverageCoef);
		super.cleanup(context);
	}

}
