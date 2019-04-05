package gr.ntua.ece.cslab.multdimwavelets.nonstandard.zpartitioning;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

public class ZOrderReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	private int counter;
	private int partitionSize;
	private Configuration conf;
	private SequenceFile.Writer writer;
	private int attemptID;
	private String outFolder;
	
	private void resetWriter() throws IllegalArgumentException, IOException{
		if(writer != null){
			writer.close();
		}
		String writerName = outFolder+"/"+attemptID+"_"+(counter / partitionSize);
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(writerName)),
				Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		attemptID = context.getTaskAttemptID().getTaskID().getId();
		conf = context.getConfiguration();
		partitionSize = Integer.parseInt(conf.get("PSIZE"));
		outFolder = conf.get("OUTFOLDER");
		counter = 0;
	}

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		if(counter % partitionSize == 0){
			resetWriter();
		}
		Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			writer.append(key, iterator.next());
		}
		counter++;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if(writer != null){
			writer.close();
		}
		super.cleanup(context);
	}

}
