package gr.ntua.ece.cslab.multdimwavelets.nonstandard;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class HyperNode implements Writable{

	protected List<NSWCoefficient> coefficientNodes;

	public HyperNode(){
		coefficientNodes = new ArrayList<NSWCoefficient>();
	}

	/**
	 * @return the coefficientNodes
	 */
	public List<NSWCoefficient> getCoefficientNodes() {
		return coefficientNodes;
	}
	/**
	 * @param coefficientNodes the coefficientNodes to set
	 */
	public void setCoefficientNodes(List<NSWCoefficient> coefficientNodes) {
		this.coefficientNodes = coefficientNodes;
	}

	public void addCoefficient(NSWCoefficient coef){
		coefficientNodes.add(coef);
	}

	@Override
	public String toString(){
		String hyperNode = "";
		boolean indexed = false;
		int count = 1;
		for(NSWCoefficient c:coefficientNodes){
			//hyperNode += c.toString()+"  ";
			if(!indexed){
				hyperNode += c.getHyperNodeIndex()+" ";
				indexed = true;
			}
			if(count < coefficientNodes.size())
				hyperNode += c.getValue()+" ";
			else
				hyperNode += c.getValue();
			count++;
		}
		return hyperNode;
	}

	@Override
	public HyperNode clone(){
		HyperNode newNode = new HyperNode();
		newNode.coefficientNodes.addAll(coefficientNodes);
		return newNode;
	}

	public void readFields(DataInput in) throws IOException {
		
		IntWritable coefsNumWritable = new IntWritable();
		coefsNumWritable.readFields(in);

		if(!coefficientNodes.isEmpty()){
			coefficientNodes.clear();
		}

		for(int i = 0; i < coefsNumWritable.get(); i++){
			NSWCoefficient coefficientWritable = new NSWCoefficient();
			coefficientWritable.readFields(in);
			coefficientNodes.add(coefficientWritable);
		}
	}

	public void write(DataOutput out) throws IOException {
		
		IntWritable coefsNumWritable = new IntWritable(coefficientNodes.size());
		coefsNumWritable.write(out);

		for(int i = 0; i < coefsNumWritable.get(); i++){
			NSWCoefficient coefficient = coefficientNodes.get(i);
			coefficient.write(out);
		}
	}

}
