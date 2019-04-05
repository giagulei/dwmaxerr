package gr.ntua.ece.cslab.multdimwavelets.thresholding.structs;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class OrderedTreeNodes {


	public static class HeapKey implements Comparable<HeapKey>{
		private static int serialNum = 0;

		private double MA;
		private int index;

		public HeapKey(double MA){
			serialNum++;
			this.MA = MA;
			this.index = serialNum;
		}

		public double getMA() {
			return MA;
		}
		public void setMA(double mA) {
			MA = mA;
		}
		public int getIndex() {
			return index;
		}
		public void setIndex(int index) {
			this.index = index;
		}

		@Override
		public boolean equals(Object o){
			HeapKey other = (HeapKey) o;
			if(this.MA==other.MA && this.index==other.index){
				return true;
			}else{return false;}
		}

		public int compareTo(HeapKey otherKey){
			if(this.MA < otherKey.MA){
				return -1;
			}else if(this.MA > otherKey.MA){
				return 1;
			}else{
				if(this.index<otherKey.index){
					return -1;
				}else if(this.index>otherKey.index){
					return 1;
				}else{
					return 0;
				}
			}
		}


	}

	private TreeMap<HeapKey, NSWCoefficient> tree;
	private HashMap<NSWCoefficient, HeapKey> index;

	public OrderedTreeNodes(){
		tree = new TreeMap<OrderedTreeNodes.HeapKey, NSWCoefficient>();
		index = new HashMap<NSWCoefficient, OrderedTreeNodes.HeapKey>();
	}


	public HashMap<NSWCoefficient, HeapKey> getIndex() {
		return index;
	}


	public void setIndex(HashMap<NSWCoefficient, HeapKey> index) {
		this.index = index;
	}


	public void createFromMap(Map<Integer, ErrortreeNodeContents> rawData){
		for(Entry<Integer, ErrortreeNodeContents> e : rawData.entrySet()){
			List<NSWCoefficient> nodeList = e.getValue().getCoefficientNodes();
			for(NSWCoefficient t:nodeList){
				if(t.getValue() != 0){
					HeapKey key = new HeapKey(t.getMA());
					index.put(t, key);
					tree.put(key, t);
				}
			}
		}
	}


	public void updatePosition(NSWCoefficient t){
		if(index.containsKey(t)){
			HeapKey oldPosition = index.get(t);
			tree.remove(oldPosition);
			oldPosition.setMA(t.getMA());
			tree.put(oldPosition, t);
		}
	}

	public boolean contains(NSWCoefficient t){
		return index.containsKey(t);
	}

	
	public NSWCoefficient poll(){
		NSWCoefficient t = null;
		if(index.size()>0){
			t = tree.pollFirstEntry().getValue();
			index.remove(t);
		}
		return t;
	}

	public int size(){
		return index.size();
	}

	public void clear(){
		index.clear();
		tree.clear();
	}


}
