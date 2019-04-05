package gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy;

import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;

public interface GreedyAbs {
	
	public void runGreedy();
	public void repositionCoefficients(ErrortreeNodeContents hypernode);
	public void updateSubtree(int root,double coeffToBeMoved);
	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error, double value, double minerror, double maxerror);
	public abstract void finalizeGreedy();

}
