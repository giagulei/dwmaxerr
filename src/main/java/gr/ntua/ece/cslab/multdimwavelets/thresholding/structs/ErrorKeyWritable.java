package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ErrorKeyWritable implements Serializable,WritableComparable<ErrorKeyWritable>,Cloneable{

	private static final long serialVersionUID = 1L;

	private int subtreeID;
	private int delOrder;
	private double maxError;


	public int getSubtreeID() {
		return subtreeID;
	}

	public void setSubtreeID(int subtreeID) {
		this.subtreeID = subtreeID;
	}

	/**
	 * @return the maxError
	 */
	public double getMaxError() {
		return maxError;
	}

	/**
	 * @param maxError the maxError to set
	 */
	public void setMaxError(double maxError) {
		this.maxError = maxError;
	}

	public int getDelOrder() {
		return delOrder;
	}

	public void setDelOrder(int delOrder) {
		this.delOrder = delOrder;
	}

	public void readFields(DataInput dataInput) throws IOException {

		IntWritable subID = new IntWritable();
		subID.readFields(dataInput);
		subtreeID = subID.get();
		
		IntWritable delOrderWritable = new IntWritable();
		delOrderWritable.readFields(dataInput);
		delOrder = delOrderWritable.get();

		DoubleWritable error = new DoubleWritable();
		error.readFields(dataInput);
		maxError = error.get();
	}

	public void write(DataOutput dataOutput) throws IOException {

		IntWritable subID = new IntWritable(subtreeID);
		subID.write(dataOutput);
		
		IntWritable delOrderWritable = new IntWritable(delOrder);
		delOrderWritable.write(dataOutput);

		DoubleWritable combinationError = new DoubleWritable(maxError);
		combinationError.write(dataOutput);
	}

	//TODO: sugkrisi me error, ektos k an einai apo to idio subtree pou diathrw th seira
	// 8elw na valw f8inonta comparator
	public int compareTo(ErrorKeyWritable other) {
		if(this.subtreeID != other.subtreeID){
			if(this.maxError > other.maxError){
				return -1;
			}
			else{
				return 1; 
			}
		}else{
			if(this.delOrder > other.delOrder){
				return -1;
			}else{ //if(this.delOrder < other.delOrder){
				return 1;
			}
			
//			else{ // den mporei idio subtree k idio delOrder
//				return 0;
//			}
		}
	}

	public ErrorKeyWritable clone(){
		ErrorKeyWritable newCombErr = new ErrorKeyWritable();
		newCombErr.setSubtreeID(subtreeID);
		newCombErr.setMaxError(maxError);
		newCombErr.setDelOrder(delOrder);
		return newCombErr;
	}


	public String toString(){
		return this.subtreeID+" "+this.maxError+" "+this.delOrder;
	}
}
