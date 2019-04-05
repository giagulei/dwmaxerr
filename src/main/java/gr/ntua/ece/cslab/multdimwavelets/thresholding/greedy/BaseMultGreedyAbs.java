package gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorBySubtreeWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.MDErrorContainer;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.OrderedTreeNodes;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.WaveletCoefArrayWritable;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;


public class BaseMultGreedyAbs<X ,Y > implements GreedyAbs{

	public static final int ERROR_BUCKET = 100;
	public static final int EMIT_BLCK_SIZE = 1000;

	protected OrderedTreeNodes pqueue;
	protected Map<Integer, ErrortreeNodeContents> errorTree;
	protected int localRootindex = 1;
	protected Double currentMaxMA;
	protected Mapper<HyperNode, NullWritable, X, Y>.Context context;
	protected int[] pows2D;

	protected Queue<MDErrorContainer> synopsis;
	protected int DEGREE_OUT;
	protected boolean distributed;
	protected Integer B;
	protected double synopsisError; //only for standalone
	
	protected MDErrorContainer deletedNode;

	protected double[] returnedErrorsPerRound;

	protected int counter;
	protected int subtreeSize;

	public BaseMultGreedyAbs(Map<Integer, ErrortreeNodeContents> errorTree, int degreeOut, boolean distributed){
		this.errorTree = errorTree;
		setPqueue(new OrderedTreeNodes());
		pqueue.createFromMap(errorTree);
		subtreeSize = this.errorTree.size();
		counter = errorTree.size() - pqueue.size(); // number of zeroes in the errortree
		setDEGREE_OUT(degreeOut);
		int levels =  (int) Math.round(Math.log((double) (errorTree.size()+1)) / Math.log(DEGREE_OUT))+1;
		pows2D = new int[levels+1];
		for (int i = 0; i < levels+1; i++) {
			pows2D[i] = (int) Math.pow(DEGREE_OUT, i);
		}
		this.distributed = distributed;
		deletedNode = new MDErrorContainer();
		returnedErrorsPerRound = new double[2];
		
		if(distributed){
			currentMaxMA = null;
		}else{
			counter = 0;
			synopsis = new LinkedList<MDErrorContainer>();
			synopsisError = 0;
			localRootindex = 1;
		}
	}

	public int getLocalRootindex() {
		return localRootindex;
	}

	public void setLocalRootindex(int localRootindex) {
		this.localRootindex = localRootindex;
	}

	public Map<Integer, ErrortreeNodeContents> getErrorTree() {
		return errorTree;
	}

	public void setErrorTree(Map<Integer, ErrortreeNodeContents> errorTree) {
		this.errorTree = errorTree;
	}

	/**
	 * @param context the context to set
	 */
	public void setContext(Mapper<HyperNode, NullWritable, X, Y>.Context context) {

		this.context = context;
	}

	public OrderedTreeNodes getPqueue() {
		return pqueue;
	}

	public void setPqueue(OrderedTreeNodes pqueue) {
		this.pqueue = pqueue;
	}

	/**
	 * @return the dEGREE_OUT
	 */
	public int getDEGREE_OUT() {
		return DEGREE_OUT;
	}

	/**
	 * @param dEGREE_OUT the dEGREE_OUT to set
	 */
	public void setDEGREE_OUT(int dEGREE_OUT) {
		DEGREE_OUT = dEGREE_OUT;
	}

	/**
	 * @return the b
	 */
	public int getB() {
		return B;
	}

	/**
	 * @param b the b to set
	 */
	public void setB(int b) {
		B = b;
	}

	public void clear(){
		pqueue.clear();
	}

	public void repositionCoefficients(ErrortreeNodeContents hypernode){
		for(NSWCoefficient tn:hypernode.getCoefficientNodes()){
			if(pqueue.contains(tn)){ // update the other nodes of the same hypernode
				hypernode.recalculateMA(tn.getIndex());
				pqueue.updatePosition(tn);
			}
		}
	}

	public void updateNode(ErrortreeNodeContents hypernode, double coeffToBeMoved){
		hypernode.updateMaxMinMatrix(coeffToBeMoved);
		repositionCoefficients(hypernode);
	}


	public void updateSubtree(int root,double coeffToBeMoved){
		ErrortreeNodeContents hypernode;
		if(root == 0) return; 
		int level=0;
		int levelStart;
		while(errorTree.containsKey(levelStart=(pows2D[level]*root))){
			for(int j=0;j< pows2D[level];j++){
				hypernode = errorTree.get(levelStart+j);
				updateNode(hypernode, coeffToBeMoved);
			}
			level++;
		}
	}

	public void addToSynopsis(MDErrorContainer err){
		synopsis.add(err);
		if(synopsis.size() > B){
			MDErrorContainer discardedNode = synopsis.poll();
			synopsisError = discardedNode.getError();
		}
	}

	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error, double value, double minerror, double maxerror){
		addToSynopsis(new MDErrorContainer(hyperNodeIndex, internalNodeIndex, error, value));
	}

	public double getSynopsisError(){
		return synopsisError;
	}

	public List<MDErrorContainer> getSynopsis(){
		if(distributed) synopsis.poll();
		return (List<MDErrorContainer>) synopsis;
	}

	public void printSynopsis(){
		try{
			PrintWriter writer = new PrintWriter("synopsis.txt", "UTF-8");
			Iterator<MDErrorContainer> synIter = synopsis.iterator();
			while(synIter.hasNext()){
				MDErrorContainer errc = synIter.next();
				writer.write("Node: "+errc.getNodeIndex()+" --> "+errc.getError()+"\n");
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected NSWCoefficient iterativeSelect(){
		NSWCoefficient heapTop = pqueue.poll();
		return heapTop;
	}

	public void testPrint(){
		for(Entry<Integer, ErrortreeNodeContents> e : errorTree.entrySet()){
			System.out.println("Node "+e.getKey());
			for(int i = 0; i < e.getValue().getNumOfChilds(); i++){
				System.out.println("Max: "+e.getValue().getMaxMinPerChild()[i][0]+" Min: "+e.getValue().getMaxMinPerChild()[i][1]);
			}
		}
	}

	protected ErrortreeNodeContents getTopMostNode(){
		return errorTree.get(localRootindex);
	}

	protected double[] iterativeUpdate(NSWCoefficient heapTop){
		double maxError, minError;
		
		//remove node with minimum MA

		int discardedHyperNodeIndx = heapTop.getHyperNodeIndex();
		ErrortreeNodeContents treenodeOfDiscardedCoef = errorTree.get(discardedHyperNodeIndx); 
		treenodeOfDiscardedCoef.updateMaxMinMatrix(heapTop.getIndex());
		repositionCoefficients(treenodeOfDiscardedCoef);

		//System.out.println("Updating subtrees..");
		int maxIterations = treenodeOfDiscardedCoef.getNumOfChilds();
		for(int i = 0; i < maxIterations; i++){
			int sign = (discardedHyperNodeIndx != 0)?-1 * NSWCoefficient.signsPerChild[heapTop.getIndex()][i]:-1;
			updateSubtree(DEGREE_OUT*discardedHyperNodeIndx+i, heapTop.getValue()*sign);
		}

		double[] maxMinError = treenodeOfDiscardedCoef.getTotalMinMax();
		maxError = maxMinError[0];
		minError = maxMinError[1];

		// update anscestors
		int child = discardedHyperNodeIndx;
		int currentHyperNodeIndex = child / DEGREE_OUT;
		ErrortreeNodeContents currentHyperNode;

		while(errorTree.containsKey(currentHyperNodeIndex)){
			currentHyperNode = errorTree.get(currentHyperNodeIndex);
			// the hyperNode where child belongs is the parentIndex-th child of the currentHyperNode
			int parentIndex; // = (child != 1)?child % DEGREE_OUT:0;

			boolean changed;

			if(child != localRootindex){
				parentIndex = child % DEGREE_OUT;
				changed = currentHyperNode.checkAndSetMax(parentIndex, maxError) | 
						currentHyperNode.checkAndSetMin(parentIndex, minError);
			}else{
				parentIndex = 0;

				changed = currentHyperNode.checkAndSetMaxRoot(parentIndex, maxError) | 
						currentHyperNode.checkAndSetMinRoot(parentIndex, minError);
			}

			if(changed){
				repositionCoefficients(currentHyperNode);

				double[] maxmin = currentHyperNode.getTotalMinMax();
				maxError = maxmin[0];
				minError = maxmin[1];

				if(currentHyperNodeIndex == 0) break;				 
				child = currentHyperNodeIndex;
				currentHyperNodeIndex = child / DEGREE_OUT;
			}else {
				break;
			}
		}

		ErrortreeNodeContents rootNode = getTopMostNode();
		double[] maxmin = rootNode.getTotalMinMax();
		returnedErrorsPerRound[0] = maxmin[0]; returnedErrorsPerRound[1] = maxmin[1];
		return returnedErrorsPerRound;
	}


	public void runGreedy(){
		//double maxError, minError;

		if(!distributed){
			if(B == null){
				System.out.println("Budget must be set in standalone mode.");
				System.exit(1);
			}
		}

		while(pqueue.size() > 0){			
			NSWCoefficient heapTop = iterativeSelect();

			double[] minMaxError = iterativeUpdate(heapTop);

			double globalMaxAbs = Math.max(Math.abs(minMaxError[0]), Math.abs(minMaxError[1])); 
			
			discardNode(heapTop.getHyperNodeIndex(), heapTop.getIndex(), globalMaxAbs, heapTop.getValue(),
					minMaxError[0], minMaxError[1]);
			
		}		
		finalizeGreedy();
	}

	// loads the errortree
	public static void loadErrorTree(Path filePath, HashMap<Integer, ErrortreeNodeContents> tree) 
			throws IllegalArgumentException, IOException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statuses = fs.listStatus(filePath);
		for(FileStatus st:statuses){
			if(st.isFile()){
				SequenceFile.Reader reader = new SequenceFile.Reader(conf,
						Reader.file(st.getPath()));
				HyperNode node = new HyperNode();

				while (reader.next(node, NullWritable.get())) {
					int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
					tree.put(hyperNodeIndex, NSWCoefficient.getGreedyTreeNodeProperties(node, hyperNodeIndex));
				}
				reader.close();
			}else{
				loadErrorTree(st.getPath(), tree);
			}
		}		
	}

	public void finalizeGreedy() {}

	public static void main(String[] args) throws IllegalArgumentException, IOException{
		
		int dim = Integer.parseInt(args[0]);
		int B = Integer.parseInt(args[1]);
		
		HashMap<Integer, ErrortreeNodeContents> errorTree = new HashMap<Integer, ErrortreeNodeContents>();
		NSWCoefficient.initializeCoefs(dim);
		long start = System.currentTimeMillis();
		BaseMultGreedyAbs.loadErrorTree(new Path("errortree"), errorTree);
		BaseMultGreedyAbs<ErrorBySubtreeWritable, WaveletCoefArrayWritable> greedyAlgo 
			= new BaseMultGreedyAbs<ErrorBySubtreeWritable, WaveletCoefArrayWritable>(errorTree, (int)Math.pow(2, dim), false);
		greedyAlgo.setB(B);
		greedyAlgo.setLocalRootindex(1);
		greedyAlgo.runGreedy();
		//greedyAlgo.printSynopsis();
		System.out.println("Execution time: "+(System.currentTimeMillis()-start)/1000L);
		System.out.println("Maximum Error: "+greedyAlgo.getSynopsisError());
		
	}

	public void discardNode(MDErrorContainer err) {
		// TODO Auto-generated method stub
		
	}

	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error){

	}

	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error, double value){

	}

//	@Override
//	public void discardNode(int hyperNodeIndex, int internalNodeIndex, double error, double minerror, double maxerror) {
//
//	}
}
