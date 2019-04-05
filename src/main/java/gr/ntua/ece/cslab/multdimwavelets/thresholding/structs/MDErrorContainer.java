package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MDErrorContainer implements Serializable,WritableComparable<MDErrorContainer>{

	public static final String INDX_DELIMITER = "_";
	
	private static final long serialVersionUID = -205464287686912501L;
	private String nodeIndex; // composite key: hypernodeID_internalID
	private double error;
	private double value;
	
	public MDErrorContainer(){}

	public MDErrorContainer(int hyperNodeIndex, int internalIndex, double error, double value){
		nodeIndex = hyperNodeIndex + INDX_DELIMITER + internalIndex;
		this.error = error;
		this.value = value;
	}

	public MDErrorContainer(String index, double error){
		nodeIndex = index;
		this.error = error;
	}
	
	public String getNodeIndex() {
		return nodeIndex;
	}
	
	public int getHyperNodeIndex(){
		return Integer.parseInt(nodeIndex.split(INDX_DELIMITER)[0]);
	}
	
	public int getInternalNodeIndex(){
		return Integer.parseInt(nodeIndex.split(INDX_DELIMITER)[1]);
	}

	public void setNodeIndex(String nodeIndex) {
		this.nodeIndex = nodeIndex;
	}
	
	public void setNodeIndex(int hyperNodeIndex, int internalIndex) {
		this.nodeIndex = hyperNodeIndex + INDX_DELIMITER + internalIndex;
	}

	public double getError() {
		return error;
	}

	public void setError(double error) {
		this.error = error;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public void readFields(DataInput dataInput) throws IOException {
		Text nodeIndexWritable = new Text();
		nodeIndexWritable.readFields(dataInput);
		nodeIndex = nodeIndexWritable.toString();

		DoubleWritable nodeError = new DoubleWritable();
		nodeError.readFields(dataInput);
		error = nodeError.get();
	}

	public void write(DataOutput dataOutput) throws IOException {		
		Text nodeIndexWritable = new Text(nodeIndex);
		nodeIndexWritable.write(dataOutput);

		DoubleWritable producedError = new DoubleWritable(error);
		producedError.write(dataOutput);
	}

	public int compareTo(MDErrorContainer o) {
		if(this.error > o.error){
			return 1;
		}else if(this.error < o.error){
			return -1;
		}else{
			return 0;
		}
	}

	public MDErrorContainer clone(){
		MDErrorContainer newErrorC = new MDErrorContainer();
		newErrorC.setNodeIndex(nodeIndex);
		newErrorC.setError(error);
		return newErrorC;
	}
	
	@Override
	public boolean equals(Object e){
		MDErrorContainer e2 = (MDErrorContainer) e;
		return nodeIndex.equals(e2.nodeIndex);
	}
	
	@Override
	public int hashCode(){
		return nodeIndex.hashCode();
	}

}
