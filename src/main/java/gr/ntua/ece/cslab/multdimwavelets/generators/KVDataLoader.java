package gr.ntua.ece.cslab.multdimwavelets.generators;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class KVDataLoader {

	public static void loadDataset(String srcLocalPath, String dstHDFSPath){
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(srcLocalPath));
			Configuration conf = new Configuration();
			Writer writer = SequenceFile.createWriter(conf, Writer.file(new Path(dstHDFSPath)),
					Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			while ((sCurrentLine = br.readLine()) != null) {
				String[] kv = sCurrentLine.split("\\s+");
				int dataValue = (int) Double.parseDouble(kv[1]);
				int k = Integer.parseInt(kv[0]);
				key.set(k);
				value.set(dataValue);
				writer.append(key, value);
			}
			writer.close();
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
		String hdfsDstPath = args[1];
		
		loadDataset(localFilePath, hdfsDstPath);
	}

}
