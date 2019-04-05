package gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;

import java.io.IOException;
import java.util.Map;


public class SynopsisMultGreedyAbs extends BaseMultGreedyAbs<ErrorKeyWritable, NSWCoefficient>{


	private ErrorKeyWritable keyToEmit;


	public SynopsisMultGreedyAbs(Map<Integer, ErrortreeNodeContents> errorTree, int degreeOut, int localRootIndex){
		super(errorTree, degreeOut, true);
		this.localRootindex = localRootIndex;
		keyToEmit = new ErrorKeyWritable();
		keyToEmit.setDelOrder(counter);
		keyToEmit.setSubtreeID(localRootIndex);
	}


	@Override
	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error, double value, double minerror, double maxerror){
		emitDiscardedNode(hyperNodeIndex, internalNodeIndex, error, minerror, maxerror);
	}
	
	private void emitDiscardedNode(int hyperNodeIndex, int internalNodeIndex, double error, double minerror, double maxerror){
		NSWCoefficient discardedCf = errorTree.get(hyperNodeIndex).getCoefficientNodes().get(internalNodeIndex);
		discardedCf.setError(error);
		discardedCf.minerror = minerror;
		discardedCf.maxerror = maxerror;

		if(counter >= subtreeSize - B){
			try {
				keyToEmit.setDelOrder(counter);
				keyToEmit.setMaxError(error);

				context.write(keyToEmit, discardedCf.clone());

			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		counter++;
	}

//	@Override
//	public void finalizeGreedy(){
//	}

}
