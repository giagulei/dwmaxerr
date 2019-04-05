package gr.ntua.ece.cslab.multdimwavelets.generators;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.Coordinates;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MDataLoader {

	public static int Z(Coordinates key){
		int z = 0;
		int [] masks = new int[key.size()];
		for (int i = 0; i < Integer.SIZE; i++) {
			for(int j=0; j < key.size(); j++){
				masks[j] = (key.get(j) & (1 << i));
				z |= (masks[j] << ((key.size()-1)*i + j));
			}
		}
		return z;
	}

	public static void loadDataset(String srcLocalPath, String dstHDFSPath, int dim){
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(srcLocalPath));

			int counter = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				String[] elements = sCurrentLine.split(",");
				Coordinates c = new Coordinates();
				for(int j=0; j < elements.length-1; j++){
					c.add(Integer.parseInt(elements[j]));
				}
				int dataValue = (int) Double.parseDouble(elements[elements.length-1]);
				System.out.println(Z(c)+","+dataValue);
				counter++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String localFilePath = args[0];
		int dim = Integer.parseInt(args[1]);
		String hdfsDstPath = args[2];
		
		loadDataset(localFilePath, hdfsDstPath, dim);
	}

}
