package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr;


import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class SynopsisIDentMapper extends Mapper<ErrorKeyWritable, NSWCoefficient, ErrorKeyWritable, NSWCoefficient>{

	@Override
	public void map(ErrorKeyWritable key, NSWCoefficient value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
	
	
}



