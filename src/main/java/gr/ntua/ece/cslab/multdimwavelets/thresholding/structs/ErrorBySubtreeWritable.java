package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ErrorBySubtreeWritable implements Serializable,WritableComparable<ErrorBySubtreeWritable>,Cloneable{

	private static final long serialVersionUID = 1L;

	private int subtreeID;
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

	public void readFields(DataInput dataInput) throws IOException {

		IntWritable subID = new IntWritable();
		subID.readFields(dataInput);
		subtreeID = subID.get();

		DoubleWritable error = new DoubleWritable();
		error.readFields(dataInput);
		maxError = error.get();
	}

	public void write(DataOutput dataOutput) throws IOException {

		IntWritable subID = new IntWritable(subtreeID);
		subID.write(dataOutput);

		DoubleWritable combinationError = new DoubleWritable(maxError);
		combinationError.write(dataOutput);
	}

	public int compareTo(ErrorBySubtreeWritable other) {

		double thisError = maxError;
		double otherError = other.maxError;

		if(thisError > otherError){
			return -1;
		}else if(thisError < otherError){
			return 1;
		}else{
			return 0;
		}
	}

	public ErrorBySubtreeWritable clone(){
		ErrorBySubtreeWritable newCombErr = new ErrorBySubtreeWritable();
		newCombErr.setSubtreeID(subtreeID);
		newCombErr.setMaxError(maxError);
		return newCombErr;
	}


}
