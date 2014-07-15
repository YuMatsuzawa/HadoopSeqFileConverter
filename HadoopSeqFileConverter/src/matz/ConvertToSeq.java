package matz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

/**MS392テキストのBzip2圧縮という形式で集めたログファイルを、SeqFileに変換するクラス。<br>
 * Valueに元データのJSONを、KeyにはTweetIDを入れて扱いやすくする。<br>
 * その際、エンコードと無駄な改行も修正する。
 * @author Matsuzawa
 *
 */
public class ConvertToSeq {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ConvertToSeq enclosure = new ConvertToSeq();
		
		File inputPath = null, outputPath = null;
		if (args.length!=2) {
			System.err.println("Usage: matz.ConvertToSeq <inputPath> <outputPath>");
			System.exit(1);
		} else {
			inputPath = new File(args[0]);
			outputPath = new File(args[1]);
			if (!inputPath.isDirectory()) {
				System.err.println("Input path is not a directory.");
				System.exit(1);
			}
		}

		ExecutorService exec = Executors.newFixedThreadPool(8);
		
		for (File inputFile : inputPath.listFiles()) {
			exec.submit(enclosure.new Converter(inputFile, outputPath));
		}
		
		exec.shutdown();
	}
	
	public class Converter implements Runnable {
		private File inputFile, outputPath;
		
		public Converter(File inputFile, File outputPath) {
			this.inputFile = inputFile;
			this.outputPath = outputPath;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			try {
				if (!this.outputPath.isDirectory()) this.outputPath.mkdir();
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(URI.create(this.outputPath.toString()), conf);
				
				Path hdpPath = new Path(this.outputPath.toString(), this.inputFile.getName().replaceFirst("\\..+", ""));
				LongWritable key = new LongWritable();
				Text val = new Text();
				SequenceFile.Writer seqfw = null;
				BufferedReader br = null;
				
				try {
					seqfw = SequenceFile.createWriter(fs, conf, hdpPath, key.getClass(), val.getClass(), 
							CompressionType.BLOCK, new DefaultCodec());
					
					//InputStreamReaderでCharsetを指定して読み込むと、その時点で変換がなされ、内部的にはStringはUTF8で扱われる。
					br = new BufferedReader(
							new InputStreamReader(
									new GZIPInputStream(
											new FileInputStream(this.inputFile)),"windows-31j"));
					
					String line = null, replaced = null;
					ObjectMapper mapper = new ObjectMapper();
					Map<String, Object> status = null;
//					Status status = null;
					int count = 0;
					while(!Thread.interrupted() && (line = br.readLine())!=null) {
						if (line.isEmpty()) continue;
						count++;

						replaced = line;
						//エスケープシーケンスと関係ない半角バックスラッシュを全角バックスラッシュに変換する。これをしないとJSONパース時に不正なエスケープシーケンスとしてエラー。
//						replaced = line.replaceAll("\\([^bfnrtu\"\\/])", "\1");
						//手前に半角バックスラッシュが存在することによりダブルクオーテーションが意図せずエスケープされてしまっている部分の対処。
//						replaced = line.replaceAll("\\\\[\"],[\"]", "\",\"");
						
						/* Regexによる不正エスケープ文字の対処にはかなり限界があるとわかったので、上記の処理は省略する。
						 * 要はJSONの公式仕様に基づきパースしようとすると問題があるだけで、文字列としては問題なく読めているので、
						 * パースに失敗した場合はkey(ユーザID)を0としてvalに元の（replaceしていない）文字列をそのまま出力する。
						 */
						
						try {
							//Twitter4jのJSONパーサーとStatusクラスを使ってもいいが、deprecatedなのと、遅い。上記エスケープシーケンスのエラーは同じように出るので、同じ仕様に従っているとわかる。
							//TwitterObjectFactoryというのが後継クラスらしい。
//							status = DataObjectFactory.createStatus(line);
//							key.set(status.getId());
							
							//JacksonJSONパーサーでJSONの中身をmapに読み込む。総称の指定がわからないのでWARNは放置。こっちのほうが相当速い。
							status = mapper.readValue(replaced, Map.class);
							key.set(Long.parseLong(status.get("id").toString()));
							val.set(replaced);
							seqfw.append(key, val);
							if (count % 10000 == 0) System.out.println(count+" tweets done.");
						} catch (JsonParseException e){
//							e.printStackTrace();
//							System.out.println(line);
//							System.out.println(">>>>>>");
//							System.out.println(replaced);
							
							key.set(0);
							val.set(line);
							seqfw.append(key, val);
							if (count % 10000 == 0) System.out.println(count+" tweets done.");
							continue;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					br.close();
					seqfw.close();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
