package gr.ntua.ece.cslab.multdimwavelets.nonstandard;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Coordinates extends ArrayList<Integer> implements Writable{
	private static final long serialVersionUID = 1L;
			
	public Coordinates(){
		super();
	}
	
	public void setCoords(int[] coords){
		for(int i = 0; i < coords.length; i++){
			if(this.size() <= i){
				this.add(i, coords[i]);
			}else{
				this.set(i, coords[i]);
			}
		}
	}
	
	public void print(){
		System.out.print("( ");
		for(int i = 0; i < this.size(); i++){
			System.out.print(this.get(i) + " ");
		}
		System.out.println(") ");
	}
	
	@Override
	public String toString(){
		String s = "( ";
		for(int i = 0; i < this.size(); i++){
			s += this.get(i) + " ";
		}
		s += ") ";
		return s;
	}
	
	@Override
	public Coordinates clone(){
		Coordinates c = new Coordinates();
		for(int i = 0; i < this.size(); i++){
			c.add(i, this.get(i));
		}
		return c;
	}
	

	public void write(DataOutput out) throws IOException {
		IntWritable coordsSize = new IntWritable(this.size());
		coordsSize.write(out);
		
		IntWritable coordElement = new IntWritable();
		for(int i = 0; i < this.size(); i++){
			coordElement.set(this.get(i));
			coordElement.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		IntWritable coordsSize  = new IntWritable();
		coordsSize.readFields(in);
		int lsize = coordsSize.get();
			
		IntWritable c = new IntWritable();
		for(int i = 0; i < lsize; i++){
			c.readFields(in);
			if(this.size() < lsize){
				this.add(c.get());
			}else{
				this.set(i, c.get());
			}
		}
	}

	
}
