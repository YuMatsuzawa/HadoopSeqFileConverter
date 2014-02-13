package matz;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class HadoopSeqFileConverter {

	private static final String bra = "{";
	private static final String cket = "}";
	private static final String dlm = ",";
	private static final char dlmChar = dlm.charAt(0);
	private static final String qt = "\"";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String inPath = "./";
		File outPath = new File(inPath);
		
		if (args.length >= 1) {
			inPath = args[0];
			if (args.length >= 2) {
				outPath = new File(args[1]);
				if (!outPath.isDirectory()) outPath.mkdir();
			}
		}

		try {
			URI uri = new URI("./");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(uri, conf);
		
			String filenameBody = "part-r-";
			String outputExtension = ".readable.txt.gz";
			for (int fileCount = 0; fileCount < 1000; fileCount++) {
				String filename = filenameBody+ String.format("%05d",fileCount);
				
				try {
					
					Path path = new Path(inPath,filename);
					
					SequenceFile.Reader seqFileReader = new SequenceFile.Reader(fs, path, conf);
					try {
						//Class<?> keyClass = seqFileReader.getKeyClass();
						//Class<?> valueClass = seqFileReader.getValueClass();
						//System.out.println(keyClass + "\t" + valueClass);
						
						Text key = new Text();
						Text val = new Text();
						File output = new File(outPath,filename + outputExtension);
						BufferedWriter bw = new BufferedWriter(
								new OutputStreamWriter(
								new GZIPOutputStream(
								new FileOutputStream(output))));

						System.out.println(path.toUri().getPath() + " > " + output.getPath());
						try {
							int userCount = 0;
							int skip = 1000;
							int lineBr = 50000;
							while(seqFileReader.next(key, val)) {
								userCount++;
								if (userCount % skip == 0) System.out.print(".");
								if (userCount % lineBr == 0) System.out.println();
								String keyString = key.toString();
								System.out.println(keyString + "=" + quoteList(val.toString()));
								bw.write(keyString + "=" + quoteList(val.toString()));
								bw.newLine();
								//break; //testing purpose
							}
							System.out.println();
							System.out.println(userCount + " users done.");
						} finally {
							bw.close();
						}
					} finally {
						seqFileReader.close();
					}
				} catch(IOException e) {
					e.printStackTrace();
					System.err.println("File or Path does not exists or cannnot be opened.");
					return;
				}
			}
		} catch(URISyntaxException e) {
			e.printStackTrace();
			System.err.println("Cannot acquire current path URI.");
			return;
		} catch(IOException e) {
			e.printStackTrace();
			System.err.println("Cannot acquire current filesystem information.");
			return;
		}
	}

	private static String quoteList(String valString) {
		String[] splitVal = valString.split(",",4);
		int numOfFollowers = Integer.parseInt(splitVal[1]);
		int numOfFriends = Integer.parseInt(splitVal[2]);
		
		if (numOfFollowers < 1 && numOfFriends < 1) {
			String empty = "";
			String FollowerList = "\"" + empty + "\"";
			
			String FriendList = "\"" + empty + "\"";
			
			String outputString =
					bra + splitVal[0]
				+ dlm + splitVal[1]
				+ dlm + splitVal[2]
				+ dlm + FollowerList
				+ dlm + FriendList + cket;
			
			return outputString;
		}
		try {
			//String[] splitList = splitVal[3].split(",",numOfFollowers + 1);
			String FollowerList = "";
			String FriendList = "";
			
			if (numOfFollowers == 0) {
				FriendList = splitVal[3];
			} else if (numOfFriends == 0) {
				FollowerList = splitVal[3];
			} else {
				int charCount = 0;
				int dlmCount = 0;
				int maxCount = splitVal[3].length();
				while(dlmCount < numOfFollowers && charCount < maxCount) {
					if (splitVal[3].charAt(charCount) == dlmChar) dlmCount ++;
					charCount++;
				}
				
				FollowerList = splitVal[3].substring(0, charCount - 1);
				FriendList = splitVal[3].substring(charCount);
			}

			String outputString =
					bra + splitVal[0]
				+ dlm + splitVal[1]
				+ dlm + splitVal[2]
				//+ dlm + qt + splitVal[3] + qt ;
				+ dlm + qt + FollowerList + qt
				+ dlm + qt + FriendList + qt + cket;
			
			//System.out.println("output:" + outputString);
			return outputString;
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
			return null;
		} catch (StringIndexOutOfBoundsException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
