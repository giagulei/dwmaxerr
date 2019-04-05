package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;

import java.util.ArrayList;
import java.util.List;

public class ErrortreeNodeContents {

	private int numOfChilds;
	private List<NSWCoefficient> coefficientNodes;

	private double[][] maxMinPerChild;
	private double[][] hypotheticErrors;
	private double[] minmax;

	public ErrortreeNodeContents(int index){
		numOfChilds = (index == 0)? 1: NSWCoefficient.degOut;
		maxMinPerChild = new double[numOfChilds][2];
		hypotheticErrors = new double[numOfChilds][2];
		minmax = new double[2];
		coefficientNodes = new ArrayList<NSWCoefficient>();
		for(int i = 0; i < numOfChilds; i++){
			maxMinPerChild[i][0] = 0;
			maxMinPerChild[i][1] = 0;
		}
	}

	public ErrortreeNodeContents(int index, List<Integer> localRootIDs){
		numOfChilds = (localRootIDs.contains(index))? 1: NSWCoefficient.degOut;
		maxMinPerChild = new double[numOfChilds][2];
		hypotheticErrors = new double[numOfChilds][2];
		minmax = new double[2];
		coefficientNodes = new ArrayList<NSWCoefficient>();
		for(int i = 0; i < numOfChilds; i++){
			maxMinPerChild[i][0] = 0;
			maxMinPerChild[i][1] = 0;
		}
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
	/**
	 * @return the maxMinPerChild
	 */
	public double[][] getMaxMinPerChild() {
		return maxMinPerChild;
	}
	/**
	 * @param maxMinPerChild the maxMinPerChild to set
	 */
	public void setMaxMinPerChild(double[][] maxMinPerChild) {
		this.maxMinPerChild = maxMinPerChild;
	}


	public int getNumOfChilds() {
		return numOfChilds;
	}

	public void setNumOfChilds(int numOfChilds) {
		this.numOfChilds = numOfChilds;
	}

	public void recalculateMA(int internalNodeIndex){
		//double[][] hypotheticErrors = new double[numOfChilds][2];
		NSWCoefficient node = coefficientNodes.get(internalNodeIndex);
		for(int i = 0; i < numOfChilds; i++){

			int sign = (numOfChilds == 1)? NSWCoefficient.zeroNodeSign : NSWCoefficient.signsPerChild[internalNodeIndex][i];

			hypotheticErrors[i][0] = Math.abs(maxMinPerChild[i][0] + sign*(-1)*node.getValue());
			hypotheticErrors[i][1] = Math.abs(maxMinPerChild[i][1] + sign*(-1)*node.getValue());
		}
		double max = Integer.MIN_VALUE;
		for(int i = 0; i < numOfChilds; i++){
			if(hypotheticErrors[i][0] > max){
				max = hypotheticErrors[i][0];
			}
			if(hypotheticErrors[i][1] > max){
				max = hypotheticErrors[i][1];
			}
		}
		node.setMA(max);
	}

	// max = double[0],   min = double[1]
	public double[] getTotalMinMax(){
		//double[] minmax = new double[2];
		minmax[0] = Integer.MIN_VALUE;
		minmax[1] = Integer.MAX_VALUE;
		for(int i = 0; i < numOfChilds; i++){
			if(maxMinPerChild[i][0] > minmax[0]){
				minmax[0] = maxMinPerChild[i][0];
			}
			if(maxMinPerChild[i][1] < minmax[1]){
				minmax[1] = maxMinPerChild[i][1];
			}
		}
		return minmax;
	}

	public void updateMaxMinMatrix(int internalNodeIndex){

		NSWCoefficient node = coefficientNodes.get(internalNodeIndex);
		for(int i = 0; i < numOfChilds; i++){

			int sign = (numOfChilds == 1)? NSWCoefficient.zeroNodeSign : NSWCoefficient.signsPerChild[internalNodeIndex][i];

			maxMinPerChild[i][0] += sign*(-1) * node.getValue();
			maxMinPerChild[i][1] += sign*(-1) * node.getValue();
		}
	}

	public void updateMaxMinMatrix(double updateValue){
		for(int i = 0; i < numOfChilds; i++){
			maxMinPerChild[i][0] += updateValue;
			maxMinPerChild[i][1] += updateValue;
		}
	}


	public void updateTreeNode(double incomingError){
		for(int i = 0; i < numOfChilds; i++){
			maxMinPerChild[i][0] = incomingError;
			maxMinPerChild[i][1] = incomingError;
		}
		for(NSWCoefficient tn: coefficientNodes){
			recalculateMA(tn.getIndex());
		}
	}


	public boolean checkAndSetMax(int childID, double newMaxError) {
		boolean changed = (newMaxError != maxMinPerChild[childID][0])? true : false;
		maxMinPerChild[childID][0] = newMaxError;
		return changed;
	}

	public boolean checkAndSetMin(int childID, double newMinError) {
		boolean changed = (newMinError != maxMinPerChild[childID][1])? true : false;
		maxMinPerChild[childID][1] = newMinError;
		return changed;
	}
	
	public boolean checkAndSetMaxRoot(int childID, double newMaxError) {
		if(newMaxError > maxMinPerChild[childID][0]){
			maxMinPerChild[childID][0] = newMaxError;
			return true;
		}
		return false;
	}

	public boolean checkAndSetMinRoot(int childID, double newMinError) {
		if(newMinError < maxMinPerChild[childID][1]){
			maxMinPerChild[childID][1] = newMinError;
			return true;
		}
		return false;
	}


}
