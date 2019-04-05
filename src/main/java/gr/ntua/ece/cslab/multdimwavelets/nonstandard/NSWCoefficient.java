package gr.ntua.ece.cslab.multdimwavelets.nonstandard;

import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class NSWCoefficient implements Writable, Comparable<NSWCoefficient>{

	public static int dim;
	public static int degOut;
	public static int[][] signsPerChild; // = {{1, 1, -1, -1}, {1, -1, -1, 1},{1, -1, 1, -1}};
	public static int zeroNodeSign = 1;
	
	private int hyperNodeIndex;
	private int index; // in the range [0-2^dim-1]
	private double value;
		
	protected double MA;

	public double minerror;
	public double maxerror;
	
	public static void initializeCoefs(int dimension){
		dim = dimension;
		computeSigns();
	}
	
	private static void computeSigns(){
		degOut  = (int) Math.pow(2, dim);
		signsPerChild = new int[degOut-1][degOut];
		HashMap<Integer, ArrayList<Integer>> signGraph = new HashMap<Integer, ArrayList<Integer>>();
		for(int i = 1; i < degOut; i++){
			int mask = 1;
			for(int d = 0; d < dim; d++){
				int ti = i & mask;
				ArrayList<Integer> s = new ArrayList<Integer>();
				if(ti == 0){s.add(1);s.add(1);}else{s.add(1);s.add(-1);}
				signGraph.put(d, s);
				mask <<= 1;
			}
			List<Integer> s = new ArrayList<Integer>();
			traverseSignGraph(signGraph, dim-1, 1, s);
			for(int k = 0; k < s.size(); k++) signsPerChild[i-1][k] = s.get(k);
			signGraph.clear();
		}
	}

	private static void traverseSignGraph(HashMap<Integer, ArrayList<Integer>> signGraph, int activeDim, int product, List<Integer> s){
		if(activeDim < 0){
			s.add(product);
			return;
		}
		traverseSignGraph(signGraph, activeDim - 1, product * signGraph.get(activeDim).get(0), s);
		traverseSignGraph(signGraph, activeDim - 1, product * signGraph.get(activeDim).get(1), s);
	}
	
	public static ErrortreeNodeContents getGreedyTreeNodeProperties(HyperNode node, int hyperNodeIndex){
		ErrortreeNodeContents treeNode = new ErrortreeNodeContents(hyperNodeIndex);
		for(int i = 0; i < node.getCoefficientNodes().size(); i++){
			NSWCoefficient coef = node.getCoefficientNodes().get(i);
			coef.setMA(Math.abs(coef.getValue()));
			treeNode.getCoefficientNodes().add(coef);
		}
		return treeNode;
	}

	public static ErrortreeNodeContents getGreedyTreeNodeProperties(HyperNode node, int hyperNodeIndex, List<Integer> localRootIDs){
		ErrortreeNodeContents treeNode = new ErrortreeNodeContents(hyperNodeIndex, localRootIDs);
		for(int i = 0; i < node.getCoefficientNodes().size(); i++){
			NSWCoefficient coef = node.getCoefficientNodes().get(i);
			coef.setMA(Math.abs(coef.getValue()));
			treeNode.getCoefficientNodes().add(coef);
		}
		return treeNode;
	}
	

	public NSWCoefficient(){};

	public NSWCoefficient(int index){
		this.index = index;
	}

	/**
	 * @return the hyperNodeIndex
	 */
	public int getHyperNodeIndex() {
		return hyperNodeIndex;
	}

	/**
	 * @param hyperNodeIndex the hyperNodeIndex to set
	 */
	public void setHyperNodeIndex(int hyperNodeIndex) {
		this.hyperNodeIndex = hyperNodeIndex;
	}

	public int getIndex() {
		return index;
	}


	public void setIndex(int index) {
		this.index = index;
	}


	public String getStringIndex(){
		return hyperNodeIndex+"_"+index;
	}
	
	/**
	 * @return the value
	 */
	public double getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(double value) {
		this.value = value;
	}

	
	public double getMA() {
		return MA;
	}

	public void setMA(double mA) {
		MA = mA;
	}
	
	public void setError(double error){
		MA = error;
	}
	
	public double getError() {
		return MA;
	}

	public void readFields(DataInput in) throws IOException {
		IntWritable hyperNodeIndexWritable = new IntWritable();
		hyperNodeIndexWritable.readFields(in);
		hyperNodeIndex = hyperNodeIndexWritable.get();
		IntWritable localIndexWritable = new IntWritable();
		localIndexWritable.readFields(in);
		index = localIndexWritable.get();
		DoubleWritable valueWritable = new DoubleWritable();
		valueWritable.readFields(in);
		value = valueWritable.get();
		DoubleWritable errorWritable = new DoubleWritable();
		errorWritable.readFields(in);
		MA = errorWritable.get();
		DoubleWritable minerrorWritable = new DoubleWritable();
		minerrorWritable.readFields(in);
		minerror = minerrorWritable.get();
		DoubleWritable maxerrorWritable = new DoubleWritable();
		maxerrorWritable.readFields(in);
		maxerror = maxerrorWritable.get();
	}

	public void write(DataOutput out) throws IOException {
		IntWritable hyperNodeIndexWritable = new IntWritable(hyperNodeIndex);
		hyperNodeIndexWritable.write(out);
		IntWritable localIndexWritable = new IntWritable(index);
		localIndexWritable.write(out);
		DoubleWritable valueWritable = new DoubleWritable(value);
		valueWritable.write(out);
		DoubleWritable errorWritable = new DoubleWritable(MA);
		errorWritable.write(out);
		DoubleWritable minerrorWritable = new DoubleWritable(minerror);
		minerrorWritable.write(out);
		DoubleWritable maxerrorWritable = new DoubleWritable(maxerror);
		maxerrorWritable.write(out);
	}

	@Override
	public int hashCode(){
		return (new Integer(hyperNodeIndex * degOut + index)).hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		NSWCoefficient otherNode = (NSWCoefficient) obj;
		if(hyperNodeIndex == otherNode.hyperNodeIndex && index == otherNode.index) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString(){
		String coef = getStringIndex()+", "+getValue()+", "+getMA();
		return coef;
	}

	public int compareTo(NSWCoefficient o) {
		if(this.index < o.index){
			return -1;
		}else if(this.index > o.index){
			return 1;
		}else return 0;
	}

	@Override
	public NSWCoefficient clone(){
		NSWCoefficient cf = new NSWCoefficient();
		cf.setHyperNodeIndex(hyperNodeIndex);
		cf.setIndex(index);
		cf.setValue(value);
		cf.setMA(MA);
		cf.minerror = minerror;
		cf.maxerror = maxerror;
		return cf;
	}
}
