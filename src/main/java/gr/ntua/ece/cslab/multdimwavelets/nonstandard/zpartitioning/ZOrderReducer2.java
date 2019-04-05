package gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ZOrderReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			context.write(key, iterator.next());
		}
		
	}
	
}
