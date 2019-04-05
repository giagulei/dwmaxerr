package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;


public class WaveletMergeCombiner<X,Y> extends WaveletMergeReducer {

	protected int count;
	protected int B;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		count = 0;
		Configuration conf = context.getConfiguration();
		B = Integer.parseInt(conf.get(WaveletConfProperties.BUDGET));
	}

	@Override
	public void reduce(ErrorKeyWritable key, Iterable<NSWCoefficient> values, Context context)
			throws IOException, InterruptedException {
			
		Iterator<NSWCoefficient> iter = values.iterator();
		while(count <= B && iter.hasNext()){	
			NSWCoefficient nextCoefficient = iter.next().clone();
			context.write(key, nextCoefficient);
			count++;
		}
	}
}
