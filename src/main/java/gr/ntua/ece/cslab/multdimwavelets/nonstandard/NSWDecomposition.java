package gr.ntua.ece.cslab.multdimwavelets.nonstandard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

// decomposition for MR
public class NSWDecomposition {

	public static final String ERRORTREE = "errortree/";
	public static final String BASE_SUBTREES_PATH = ERRORTREE+"subtrees/";

	protected double[] W;
	protected List<HyperNode> errortree;

	private int localRootIndex;
	protected int insertIndex;
	private double topLevelAverage; 

	private int blockID;
	protected int d; //dimensionality
	private int N;
	protected int sbb;

	
	public NSWDecomposition(int partitionSize, int d){
		W = new double[partitionSize];
		this.d = d;
		sbb = (int) Math.pow(2, d); //sbb: size of the d-dimensional basic block
		errortree = new LinkedList<HyperNode>();
		insertIndex = 0;
	}

	public List<HyperNode> getErrorTree(){
		return errortree;
	}
	
	public int getN() {
		return N;
	}

	public void setN(int N) {
		this.N = N;
	}

	public int getLocalRootIndex() {
		return localRootIndex;
	}

	public void setLocalRootIndex(int localRootIndex) {
		this.localRootIndex = localRootIndex;
	}

	public double getTopLevelAverage() {
		return topLevelAverage;
	}

	public void setTopLevelAverage(double topLevelAverage) {
		this.topLevelAverage = topLevelAverage;
	}

	public int getBlockID() {
		return blockID;
	}

	public void setBlockID(int blockID) {
		this.blockID = blockID;
	}

	public int getSBB(){
		return sbb;
	}

	public void addTuple(double value){
		W[insertIndex] = value;
		insertIndex++;
	}


	// adds the nodes of the current level to the tree and returns the array with the averages for 
	// further resolution
	public double[] createWavelet(double[] wArray, int initialTreeIndex){
		int newLevelPsize = wArray.length / sbb;
		int treeIndex = initialTreeIndex;

		setLocalRootIndex(treeIndex);
		double[] averages = new double[newLevelPsize];
		int avgIndex = 0;
		HashSet<Integer> used = new HashSet<Integer>();
		for(int i = 0; i < wArray.length; i += sbb){
			//a new d-dimensional basic blocks begins. Initialize appropriate structures
			HyperNode currentHyperNode = new HyperNode();

			for(int dim = 0; dim < d; dim++){
				int groupjump = (int) Math.pow(2, dim);
				for(int k = 0; k < sbb; k++){
					if(used.contains(i+k)) continue;
					int firstIndex = i+k;
					int secondIndex = firstIndex + groupjump;
					double avg = (wArray[firstIndex] + wArray[secondIndex])/2;
					double coef = (wArray[firstIndex] - wArray[secondIndex])/2;
					wArray[firstIndex] = avg;
					wArray[secondIndex] = coef;

					used.add(firstIndex);
					used.add(secondIndex);
				}
				used.clear();
			}

			//add average to the return array
			averages[avgIndex] = wArray[i];
			avgIndex++;
			// add the transformed coefficients to the corresponding hypernode

			for(int coefIndex = i+1; coefIndex < i+sbb; coefIndex++){
				NSWCoefficient coefficient = new NSWCoefficient(coefIndex-i-1);
				coefficient.setHyperNodeIndex(treeIndex);
				coefficient.setValue(wArray[coefIndex]);
				currentHyperNode.addCoefficient(coefficient);
			}
			// add the hypernode to the tree
			errortree.add(currentHyperNode);treeIndex++;
		}
		return averages;
	} 

	public void transform(){
		double[] currentArray = W;
		int level = (int) Math.round(Math.log((double) N/sbb) / Math.log(sbb));
		
		int newLevelPsize, treeIndex;
	
		while(currentArray.length > 1){
			newLevelPsize = currentArray.length / sbb;
			treeIndex = (int)Math.pow(sbb, level) + blockID *newLevelPsize;
			currentArray = createWavelet(currentArray, treeIndex);
			level--;
		}
		setTopLevelAverage(currentArray[0]);
	}

	public void persistSubTree(Path filenamePath) throws IOException{
		Configuration config = new Configuration();
		SequenceFile.Writer writer = SequenceFile.createWriter(config, Writer.file(filenamePath),
				Writer.keyClass(HyperNode.class), Writer.valueClass(NullWritable.class));

		for(HyperNode node:errortree){
			writer.append(node, NullWritable.get());
		}

		writer.close();
	}
	
	public void persistSubTree(Path filenamePath, NSWCoefficient rootCoef) throws IOException{
		Configuration config = new Configuration();
		SequenceFile.Writer writer = SequenceFile.createWriter(config, Writer.file(filenamePath),
				Writer.keyClass(HyperNode.class), Writer.valueClass(NullWritable.class));

		for(HyperNode node:errortree){
			writer.append(node, NullWritable.get());
		}
		
		rootCoef.setIndex(0);
		HyperNode h =  new HyperNode();
		List<NSWCoefficient> coefs = new ArrayList<NSWCoefficient>();
		coefs.add(rootCoef);
		h.setCoefficientNodes(coefs);
		writer.append(h, NullWritable.get());
		
		writer.close();
	}


	public static void main(String[] args) {
		String filename = args[0];
		int dim = Integer.parseInt(args[1]);

		BufferedReader br = null;
		FileReader fr = null;
		ArrayList<Double> indata = new ArrayList();
		try{
			fr = new FileReader(filename);
			br = new BufferedReader(fr);
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				indata.add(Double.parseDouble(sCurrentLine));
			}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if(br != null) br.close();
			        if(fr != null) fr.close();
			}catch(IOException e){
				e.printStackTrace();
			}	
		}

		System.out.println("N = "+indata.size()+" dim = "+dim);

		NSWDecomposition dec = new NSWDecomposition(indata.size(), dim);
		dec.setN(indata.size());
		for(int i = 0; i< indata.size(); i++){
			dec.addTuple(indata.get(i));
		}
		dec.transform();
		NSWCoefficient rootCoef = new NSWCoefficient(0);
		rootCoef.setIndex(0);
		rootCoef.setHyperNodeIndex(0);
		rootCoef.setValue(dec.topLevelAverage);
		HyperNode h =  new HyperNode();
		List<NSWCoefficient> coefs = new ArrayList<NSWCoefficient>();
		coefs.add(rootCoef);
		h.setCoefficientNodes(coefs);
		dec.errortree.add(h);

		for(HyperNode hyp: dec.errortree){
			System.out.println(hyp.toString());
		}

	}

}
