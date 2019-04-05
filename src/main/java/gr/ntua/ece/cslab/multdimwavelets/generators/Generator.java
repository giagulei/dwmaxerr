package gr.ntua.ece.cslab.multdimwavelets.generators;



import org.apache.hadoop.io.SequenceFile;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.Coordinates;

public abstract class Generator {

	protected int[] dims;
	protected long N;
	protected SequenceFile.Writer writer;
	protected String file;

	public Generator(String file, long N, int[] dims){
		setFile(file);
		setN(N);
		setDims(dims);
	}

	public int[] getDims() {
		return dims;
	}

	public void setDims(int[] dims) {
		this.dims = dims;
	}

	public long getN() {
		return N;
	}

	public void setN(long n) {
		N = n;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public abstract void generate();

	public Coordinates nextCoords(Coordinates currentPoint){
		while(true){
			//start always from 0-dim
			if(currentPoint.get(0) + 1 < dims[0]){
				currentPoint.set(0, currentPoint.get(0) + 1);
				return currentPoint;
			}
			// find next available dimension
			int nextDim = 1;
			while(nextDim < currentPoint.size() && currentPoint.get(nextDim) + 1 >= dims[nextDim]){
				nextDim++;
			}
			if(nextDim < dims.length){
				currentPoint.set(nextDim, currentPoint.get(nextDim) + 1);
				// set to zero all previous dims
				for(int j = 0; j < nextDim; j++) currentPoint.set(j, 0);
				return currentPoint;
			}else{
				return null; // no available point. stop iteration
			}
		}
	}

	public static void main(String[] args){
		int[] dims = {16, 16};
		Generator g = new Generator("lala", 256,dims) {

			@Override
			public void generate() {
				// TODO Auto-generated method stub

			}
		};

		Coordinates initialCoord = new Coordinates();
		int dimensionality = dims.length;
		for(int i = 0; i < dimensionality; i++){
			initialCoord.add(0);
		}

		do{
			System.out.println(initialCoord.toString());
		}while((initialCoord = g.nextCoords(initialCoord)) != null);

	}
}
