package gr.ntua.ece.cslab.multdimwavelets.generators;

import java.io.IOException;
import java.util.ArrayList;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.WaveletCoefArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class DReader {

	public static void readHypernodeSubtrees(String path) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(new Path(path)));

		HyperNode node = new HyperNode();

		while (reader.next(node, NullWritable.get())) {
			System.out.println(node.toString());
		}

		reader.close();

	}
	
	
	public static void readZPartitioning(String path) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(new Path(path)));

		IntWritable coord = new IntWritable();
		IntWritable val = new IntWritable();

		while (reader.next(coord, val)) {
			System.out.println(coord.get()+"  "+val.get());
			//coord = new Coordinates();
		}

		reader.close();

	}
	
	public static void synopsisReader(String path) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(new Path(path)));

		NSWCoefficient node = new NSWCoefficient();
		int counter = 1;
		while (reader.next(node, NullWritable.get())) {
			//System.out.println(node.toString());
			System.out.println(counter+" "+node.getError());
			counter++;
		}

		reader.close();

	}
	
	public static void readSynopsisIndex(String path) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(new Path(path)));
		int count = 0;
		WaveletCoefArrayWritable coefArray = new WaveletCoefArrayWritable();
		IntWritable subtreeIndex = new IntWritable();
		while(reader.next(subtreeIndex, coefArray)){
			System.out.print("Subtree "+subtreeIndex.get()+": ");
			for(NSWCoefficient c : coefArray.getCoefficientsList()){
				count++;
				System.out.print(c.toString()+"  ,  ");
			}
			System.out.println();
		}
		System.out.println(count+" coefs are kept.");
		reader.close();
	}


	public static void main(String[] args) throws IllegalArgumentException, IOException {
		int option = Integer.parseInt(args[1]);
		if(option == 0)
			readHypernodeSubtrees(args[0]);
		else if(option == 1)
			readZPartitioning(args[0]);
		else if(option == 2){
			readSynopsisIndex(args[0]);
		}else
			synopsisReader(args[0]);
	}

}
