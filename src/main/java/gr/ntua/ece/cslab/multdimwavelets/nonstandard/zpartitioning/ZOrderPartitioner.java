package gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author giagulei
 *
 */
public class ZOrderPartitioner extends Partitioner<IntWritable, IntWritable> implements Configurable{

	private int blockSize;
	private Configuration conf;
	
	@Override
	public int getPartition(IntWritable key, IntWritable arg1, int numOfReducers) {
		return (key.get()/blockSize) % numOfReducers;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		blockSize = Integer.parseInt(conf.get("PSIZE"));
	}
}
