package gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning;

import java.io.IOException;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.Coordinates;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ZOrderMapper extends Mapper<Coordinates, IntWritable, IntWritable, IntWritable>  {

	public void map(Coordinates key, IntWritable value, Context context) throws IOException, InterruptedException {
		int z = 0;
		int [] masks = new int[key.size()];
		for (int i = 0; i < Integer.SIZE; i++) {
			for(int j=0; j < key.size(); j++){
				masks[j] = (key.get(j) & (1 << i));
				z |= (masks[j] << ((key.size()-1)*i + j));
			}
		}
		context.write(new IntWritable(z), value);
	}
	

}
