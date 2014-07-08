package matz;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**SeqFileを冒頭だけ試験的に読んで中身を確かめるためのクラス。
 * @author Matsuzawa
 *
 */
public class ReadSeqTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File input = null;
		if (args.length == 0) {
			System.err.println("Usage: matz.ReadSeqTest <inputPath or inputFile>");
			System.exit(1);
		} else {
			input = new File(args[0]);
			if(!input.exists()) {
				System.err.println("Input does not exist.");
				System.exit(1);
			}
		}

		File inputFile = null;
		if(input.isDirectory()) inputFile = input.listFiles()[0]; //試験的に入力ファイルを1つだけ取得
		else inputFile = input;
		
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(input.toString()), conf);
			Path file = new Path(inputFile.toString());
			SequenceFile.Reader seqfr = null;
			try {
				seqfr = new SequenceFile.Reader(fs, file, conf);
				
				LongWritable key = new LongWritable();
				Text val = new Text();
				for(int i=0; i<20; i++) {
					seqfr.next(key, val);
					System.out.println(key.toString());
					System.out.println("\t"+val.toString());
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				seqfr.close();
			}
		
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
