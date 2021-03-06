package gr.ntua.ece.cslab.multdimwavelets.generators;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.Coordinates;

public class UniformGenerator extends Generator{
	
	private int maxValue;

	public UniformGenerator(String file, long N, int[] dims) {
		super(file, N, dims);
	}

	public int getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(int maxValue) {
		this.maxValue = maxValue;
	}

	@Override
	public void generate() {
		Coordinates initialCoord = new Coordinates();
		int dimensionality = dims.length;
		if(dimensionality == 1){
			generate1D(dims[0]);
			return;
		}
		for(int i = 0; i < dimensionality; i++){
			initialCoord.add(0);
		}
		try {
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			Path filenamePath = new Path(file);
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}
			SequenceFile.Writer writer = SequenceFile.createWriter(config, Writer.file(filenamePath),
					Writer.keyClass(Coordinates.class), Writer.valueClass(IntWritable.class));
			Random r = new Random(System.currentTimeMillis());
			IntWritable value = new IntWritable();
			
			do{
				value.set(r.nextInt(maxValue));
				writer.append(initialCoord, value);
			}while((initialCoord = nextCoords(initialCoord)) != null);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void generate1D(int length){
		try {
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			Path filenamePath = new Path(file);
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}
			SequenceFile.Writer writer = SequenceFile.createWriter(config, Writer.file(filenamePath),
					Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));
			Random r = new Random(System.currentTimeMillis());
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			int counter = 0;
			do{
				key.set(counter);
				value.set(r.nextInt(maxValue));
				writer.append(key, value);
				counter++;
			}while(counter < length);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		if(args.length < 4){
			System.out.println("Wrong arguments");
			System.exit(0);
		}
		
		String outputFile = args[0];
		long N = Long.parseLong(args[1]);
		int maxValue = Integer.parseInt(args[2]);
		int[] dims = new int[args.length - 3];
		for(int i = 3; i < args.length; i++)
			dims[i-3] = Integer.parseInt(args[i]);
		
		System.out.println("output: "+outputFile);
		System.out.println("datasize: "+N);
		System.out.println("Max value: "+maxValue);
		System.out.print("Dims: ");
		for(int i = 0; i < dims.length; i++) System.out.print(dims[i]+" ");System.out.println();
		UniformGenerator generator = new UniformGenerator(outputFile, N, dims);
		generator.setMaxValue(maxValue);
		
		generator.generate();

	}


}
