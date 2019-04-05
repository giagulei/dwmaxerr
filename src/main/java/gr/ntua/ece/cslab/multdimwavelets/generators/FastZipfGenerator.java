package gr.ntua.ece.cslab.multdimwavelets.generators;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class FastZipfGenerator
{
    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map;

    FastZipfGenerator(int size, double skew)
    {
        map = computeMap(size, skew);
    }

    private static NavigableMap<Double, Integer> computeMap(
        int size, double skew)
    {
        NavigableMap<Double, Integer> map = 
            new TreeMap<Double, Integer>();

        double div = 0;
        for (int i = 1; i <= size; i++)
        {
            div += (1 / Math.pow(i, skew));
        }

        double sum = 0;
        for(int i=1; i<=size; i++)
        {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum,  i-1);
        }
        return map;
    }

    public double next()
    {
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue()+1;
    }
    
    public static void main(String[] args){
	int N = Integer.parseInt(args[0]);
    	FastZipfGenerator zipf1 = new FastZipfGenerator(8192, 1.5);
    		
    	for(int i=0;i<N;i++){
    		int sample1 = (int) zipf1.next();
		int sample2 = (int) zipf1.next();
		System.out.println(sample1+","+sample2);
    	}
    	
    	
    }

}
