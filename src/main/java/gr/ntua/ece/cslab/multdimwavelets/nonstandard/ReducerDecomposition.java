package gr.ntua.ece.cslab.multdimwavelets.nonstandard;


// decomposition for MR
public class ReducerDecomposition extends NSWDecomposition{
	

	public ReducerDecomposition(int partitionSize, int d){
		super(partitionSize, d);
	}
	
	@Override
	public void transform(){
		double[] currentArray = W;
		int treeIndex;
		while(currentArray.length > 1){
			treeIndex = currentArray.length / sbb;
			currentArray = createWavelet(currentArray, treeIndex);
		}
		NSWCoefficient rootCoef = new NSWCoefficient(0);
		rootCoef.setHyperNodeIndex(0);
		rootCoef.setValue(currentArray[0]);
		
		HyperNode root = new HyperNode();
		root.addCoefficient(rootCoef);
		errortree.add(root);
	}
	
}
