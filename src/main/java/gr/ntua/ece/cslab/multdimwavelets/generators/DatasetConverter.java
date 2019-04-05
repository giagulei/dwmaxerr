package gr.ntua.ece.cslab.multdimwavelets.generators;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.Coordinates;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

public class DatasetConverter {

	private String input;
	private String output; // folder
	private int splitSize;

	private HashMap<String, SequenceFile.Writer> blockWriters; 

	public DatasetConverter(){
		blockWriters = new HashMap<String, SequenceFile.Writer>();
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public int getSplitSize() {
		return splitSize;
	}

	public void setSplitSize(int splitSize) {
		this.splitSize = splitSize;
	}
	
	public SequenceFile.Writer openWriter(String name, Coordinates coord) throws IOException{
		Configuration config = new Configuration();	
		String firstCoord = "";
		for(int i = 0; i < coord.size(); i++) firstCoord += "_"+coord.get(i);
		Path filenamePath = new Path(output+"/"+name+firstCoord);
		SequenceFile.Writer writer = SequenceFile.createWriter(config, Writer.file(filenamePath),
				Writer.keyClass(Coordinates.class), Writer.valueClass(DoubleWritable.class));
		blockWriters.put(name, writer);
		return writer;
	}

	public void convert() throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(new Path(input)));

		Coordinates coord = new Coordinates();
		DoubleWritable val = new DoubleWritable();

		while (reader.next(coord, val)) {
			String writerIndex = "";
			for(int i = 0; i < coord.size(); i++){
				writerIndex += Integer.toString(coord.get(i) / splitSize); 
			}
			SequenceFile.Writer currentWriter;
			if(!blockWriters.containsKey(writerIndex)){
				currentWriter = openWriter(writerIndex, coord);
			}else{
				currentWriter = blockWriters.get(writerIndex);
			}
			currentWriter.append(coord, val);
		}
		
		for(SequenceFile.Writer writer:blockWriters.values()){
			writer.close();
		}

		reader.close();
	}

	public static void main(String[] args) {
		
		if( args.length != 3 ){
			System.out.println("Wrong arguments");
			System.exit(0);
		}
		
		String input = args[0];
		String output = args[1];
		int splitSizePerDimension = Integer.parseInt(args[2]);
		DatasetConverter converter = new DatasetConverter();
		converter.setInput(input);
		converter.setOutput(output);
		converter.setSplitSize(splitSizePerDimension);
	}

}
