package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.dgreedyabs;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.AbstractWaveletMapper;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;

public abstract class AbstractCombGreedyMapper<X, T> extends AbstractWaveletMapper<HyperNode, NullWritable, X, T>{

	
	public ArrayList<Integer> getPathIndices(int localRootIndex){
		ArrayList<Integer> pathIndices = new ArrayList<Integer>();
		int parent = localRootIndex;
		while((parent /= DEGREE_OUT) > 0){
			pathIndices.add(parent);
		}
		pathIndices.add(0);
		return pathIndices;
	}

	public double computeIncomingError(int child, BitSet combination, HashMap<Integer, HyperNode> rootErrorTree){
		double incomingError = 0;
		int currentNode = child;
		int parent = currentNode/DEGREE_OUT;


		while(currentNode > 1){
			if(currentNode != 0){
				int hyperNodeIndex = rootErrorTree.get(parent).getCoefficientNodes().get(0).getHyperNodeIndex();
				ErrortreeNodeContents nodeCoefs = NSWCoefficient.getGreedyTreeNodeProperties(rootErrorTree.get(parent), hyperNodeIndex);
				
				if(currentNode != 1){
					int parentSignIndex = currentNode % DEGREE_OUT;
					for(int i = 0; i < DEGREE_OUT-1; i++){
						int combIndex = parent * DEGREE_OUT + i;
						
						if(!combination.get(combIndex)){
							NSWCoefficient tn = nodeCoefs.getCoefficientNodes().get(i);
							incomingError += tn.getValue() * NSWCoefficient.signsPerChild[tn.getIndex()][parentSignIndex]*(-1);
						}
					}
				}else{
					for(int i = 0; i < DEGREE_OUT-1; i++){
						int combIndex = parent * DEGREE_OUT + i;
						
						if(!combination.get(combIndex)){
							NSWCoefficient tn = nodeCoefs.getCoefficientNodes().get(i);
							incomingError += tn.getValue() * NSWCoefficient.zeroNodeSign*(-1);
						}
					}
				}
				
				currentNode = parent;
				parent = currentNode/DEGREE_OUT;
			}else{
				break;
			}
		}
		if(!combination.get(0)){
			int hyperNodeIndex = rootErrorTree.get(parent).getCoefficientNodes().get(0).getHyperNodeIndex();
			ErrortreeNodeContents nodeCoefs = NSWCoefficient.getGreedyTreeNodeProperties(rootErrorTree.get(parent), hyperNodeIndex);
			NSWCoefficient tn = nodeCoefs.getCoefficientNodes().get(0);
			incomingError += tn.getValue()*(-1);
		}
		return incomingError; 
	}

	public void updateTreeNodes(double incomingError, HashMap<Integer, ErrortreeNodeContents> tree){
		for(Entry<Integer, ErrortreeNodeContents> treenode:tree.entrySet()){
			ErrortreeNodeContents coefs = treenode.getValue();
			coefs.updateTreeNode(incomingError);
		}
	}
	
	public BitSet computePathToRoot(int localRoot){
		ArrayList<Integer> pathIndices = getPathIndices(localRoot);
		BitSet path = new BitSet();
		for(Integer p:pathIndices){
			if(p == 0){
				path.set(p);
				continue;
			}
			for(int i = 0; i < DEGREE_OUT-1; i++){
				path.set(DEGREE_OUT * p + i);
			}
		}
		return path;
	}
	
	
}



