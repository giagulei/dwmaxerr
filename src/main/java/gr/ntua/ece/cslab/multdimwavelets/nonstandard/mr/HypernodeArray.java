package gr.ntua.ece.cslab.multdimwavelets.nonstandard.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;


public class HypernodeArray implements Writable{
	
	private ArrayList<HyperNode> coefficientsList;

	public HypernodeArray() {
		coefficientsList = new ArrayList<HyperNode>();
	}
	
	public ArrayList<HyperNode> getCoefficientsList() {
		return coefficientsList;
	}

	public void addCoefficient(HyperNode coefficient){
		coefficientsList.add(coefficient);
	}
	
	public void addAllCoefficients(HypernodeArray array){
		coefficientsList.addAll(array.getCoefficientsList());
	}
	
	public void readFields(DataInput in) throws IOException {
		coefficientsList.clear();
		IntWritable listSizeWritable = new IntWritable();
		listSizeWritable.readFields(in);
		
		for(int i = 0; i < listSizeWritable.get(); i++){
			HyperNode coef = new HyperNode();
			coef.readFields(in);
			coefficientsList.add(coef);
		}
	}

	public void write(DataOutput out) throws IOException {
		IntWritable listSizeWritable = new IntWritable(coefficientsList.size());
		listSizeWritable.write(out);
		
		for(int i = 0; i < listSizeWritable.get(); i++){
			HyperNode coef = coefficientsList.get(i);
			coef.write(out);
		}
	}
	
	public HypernodeArray clone(){
		HypernodeArray newWArray = new HypernodeArray();
		newWArray.coefficientsList.addAll(this.coefficientsList);
		return newWArray;
	}


}
