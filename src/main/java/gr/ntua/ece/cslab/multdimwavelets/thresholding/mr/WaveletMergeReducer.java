package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;


public abstract class WaveletMergeReducer extends
Reducer<ErrorKeyWritable, NSWCoefficient, ErrorKeyWritable, NSWCoefficient> {


	protected int count;
	protected int B;
	protected int[] subtreeIDs;
	protected int dim;
	
	protected boolean result;

	protected List<NSWCoefficient> synopsis;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		synopsis = new ArrayList<NSWCoefficient>();
		count = 0;
		Configuration conf = context.getConfiguration();
		dim = Integer.parseInt(conf.get(WaveletConfProperties.DIM));
		B = Integer.parseInt(conf.get(WaveletConfProperties.BUDGET));
		result = Boolean.parseBoolean(conf.get("result"));
		
		String subtrees = conf.get(WaveletConfProperties.SUBTREE_IDS);
		String [] subtreeRoots = subtrees.split(",");
		subtreeIDs = new int[subtreeRoots.length];
		for(int i = 0; i < subtreeRoots.length; i++){
			subtreeIDs[i] = Integer.parseInt(subtreeRoots[i]);
		}
	}

	@Override
	public void reduce(ErrorKeyWritable key, Iterable<NSWCoefficient> values, Context context)
			throws IOException, InterruptedException {
			
		Iterator<NSWCoefficient> iter = values.iterator();
		while(count <= B && iter.hasNext()){	
			NSWCoefficient nextCoefficient = iter.next().clone();
			context.write(key, nextCoefficient);
			synopsis.add(nextCoefficient);
			count++;
			if(count == B && result){
				writeFinalErrorToHDFS("FINAL_ERROR", nextCoefficient.getError(), context);
			}
		}
	}

	public void writeFinalErrorToHDFS(String synopsisErrorFile, double error, Context context) throws IllegalArgumentException, IOException{
		SequenceFile.Writer writer = SequenceFile.createWriter(context.getConfiguration(), Writer.file(new Path(synopsisErrorFile)),
				Writer.keyClass(DoubleWritable.class), Writer.valueClass(NullWritable.class));
		writer.append(new DoubleWritable(error), NullWritable.get());
		writer.close();
	}
	

}
