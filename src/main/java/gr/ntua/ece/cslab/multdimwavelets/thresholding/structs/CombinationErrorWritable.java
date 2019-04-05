package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class CombinationErrorWritable implements Serializable,WritableComparable<CombinationErrorWritable>,Cloneable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private BitSet combination;
	private int delOrder;
	private double maxError;
	private int subtreeID;

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

	/**
	 * @return the combination
	 */
	public BitSet getCombination() {
		return combination;
	}

	/**
	 * @param combination the combination to set
	 */
	public void setCombination(BitSet combination) {
		this.combination = combination;
	}

	/**
	 * @return the delOrder
	 */
	public int getDelOrder() {
		return delOrder;
	}

	/**
	 * @param delOrder the delOrder to set
	 */
	public void setDelOrder(int delOrder) {
		this.delOrder = delOrder;
	}
	
	
	public void readFields(DataInput dataInput) throws IOException {

		IntWritable iw = new IntWritable();
		DoubleWritable dw = new DoubleWritable();

		BytesWritable combinationBytes = new BytesWritable();
		combinationBytes.readFields(dataInput);
		byte[] thisCombinationBytes = combinationBytes.getBytes();
		combination = BitSet.valueOf(thisCombinationBytes);

		iw.readFields(dataInput); subtreeID = iw.get();
		iw.readFields(dataInput); delOrder = iw.get();
		dw.readFields(dataInput); maxError = dw.get();
	}

	
	public void write(DataOutput dataOutput) throws IOException {

		IntWritable iw = new IntWritable();
		DoubleWritable dw = new DoubleWritable(maxError);

		byte[] thisCombinationBytes = combination.toByteArray();
		BytesWritable combinationBytes = new BytesWritable(thisCombinationBytes);
		combinationBytes.write(dataOutput);

		iw.set(subtreeID);iw.write(dataOutput);
		iw.set(delOrder);iw.write(dataOutput);
		dw.write(dataOutput);
	}

	
	public int compareTo(CombinationErrorWritable other) {
		
		byte[] thisBytes = combination.toByteArray();
		byte[] otherBytes = other.combination.toByteArray();
		
		int comparisonResult = compare(thisBytes, otherBytes);
		
		if(comparisonResult!=0){
			return comparisonResult;
		}else{
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
			}
		}

	}

	public static int compare(byte[] left, byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}
	
//	public byte[] getBytes(){
//		byte[] combinationBytes = combination.toByteArray();
//		 int totalSize = Integer.SIZE/8 + combinationBytes.length*Byte.SIZE/8
//				 + Double.SIZE/8;
//
//         byte[] serializable = new byte[totalSize];
//
//         ByteBuffer buffer = ByteBuffer.wrap(serializable);
//
//         buffer.putInt(combinationBytes.length);
//
//         buffer.put(combinationBytes);
//         buffer.putDouble(delOrder);
//
//         return serializable;
//	}
//
//	public void parseBytes(byte[] bytes){
//		  int index = 0;
//          ByteBuffer buffer = ByteBuffer.wrap(bytes);
//
//          int combinationSize = buffer.getInt(index);
//          index+= Integer.SIZE/8;
//          byte[] cbytes = new byte[combinationSize];
//          for(int i=0;i<combinationSize;i++){
//        	  cbytes[i] = bytes[index+i];
//          }
//          combination = BitSet.valueOf(cbytes);
//          index+= combinationSize*Byte.SIZE/8;
//
//          delOrder = buffer.getDouble(index);
//	}
	
	public CombinationErrorWritable clone(){
		CombinationErrorWritable newCombErr = new CombinationErrorWritable();
		newCombErr.setCombination(combination);
		newCombErr.setDelOrder(delOrder);
		newCombErr.setMaxError(maxError);
		newCombErr.setSubtreeID(subtreeID);
		return newCombErr;
	}
	

}
