package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;


public class WaveletCoefArrayWritable implements Writable{
	
	private ArrayList<NSWCoefficient> coefficientsList;
	private int subtreeID;

	public WaveletCoefArrayWritable() {
		coefficientsList = new ArrayList<NSWCoefficient>();
	}
	
	public ArrayList<NSWCoefficient> getCoefficientsList() {
		return coefficientsList;
	}

	/**
	 * @return the subtreeID
	 */
	public int getSubtreeID() {
		return subtreeID;
	}

	/**
	 * @param subtreeID the subtreeID to set
	 */
	public void setSubtreeID(int subtreeID) {
		this.subtreeID = subtreeID;
	}

	public void addCoefficient(NSWCoefficient coefficient){
		coefficientsList.add(coefficient);
	}
	
	public void addAllCoefficients(WaveletCoefArrayWritable array){
		coefficientsList.addAll(array.getCoefficientsList());
	}
	
	public NSWCoefficient get(int index){
		return coefficientsList.get(index);
	}
	
	public int size(){
		return coefficientsList.size();
	}
	
	public boolean isEmpty(){
		return coefficientsList.isEmpty();
	}
	
	public void reset(){
		coefficientsList.clear();
	}
	
	public void reverse(){
		Collections.reverse(coefficientsList);
	}
	
	public String toString(){
		String list = "";
		for(int i = 0; i < coefficientsList.size(); i++)
			list += " "+coefficientsList.get(i).getError();
		return list;
	}
	
	
	public void readFields(DataInput in) throws IOException {
		IntWritable subtreeIDWritable = new IntWritable();
		subtreeIDWritable.readFields(in);
		subtreeID = subtreeIDWritable.get();
		
		coefficientsList.clear();
		IntWritable listSizeWritable = new IntWritable();
		listSizeWritable.readFields(in);
		
		for(int i = 0; i < listSizeWritable.get(); i++){
			NSWCoefficient coef = new NSWCoefficient();
			coef.readFields(in);
			coefficientsList.add(coef);
		}
	}

	public void write(DataOutput out) throws IOException {
		IntWritable subtreeIDWritable = new IntWritable(subtreeID);
		subtreeIDWritable.write(out);
		
		IntWritable listSizeWritable = new IntWritable(coefficientsList.size());
		listSizeWritable.write(out);
		
		for(int i = 0; i < listSizeWritable.get(); i++){
			NSWCoefficient coef = coefficientsList.get(i);
			coef.write(out);
		}
	}
	
	public WaveletCoefArrayWritable clone(){
		WaveletCoefArrayWritable newWArray = new WaveletCoefArrayWritable();
		newWArray.coefficientsList.addAll(this.coefficientsList);
		newWArray.setSubtreeID(subtreeID);
		return newWArray;
	}


}
