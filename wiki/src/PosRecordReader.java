import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

public class PosRecordReader implements RecordReader<Text, PartialString> {

	private Integer offset = 0;
	private FileSystem fileSys;
	private FSDataInputStream fsStreamBigFile;
	private FSDataInputStream fsStreamPatt;
	private Scanner scan;
	private String patt;
	private String FileName;
	
	private FileSplit split;
	private JobConf job;
	/**
	 * @param lineReader
	 * @param lineKey
	 * @param lineValue
	 */
	public PosRecordReader(JobConf job, FileSplit split) throws IOException {
		this.split = split;
		this.job = job;
		
		scan = new Scanner(new File("./query.txt"));
	}

	public boolean next(Text key, PartialString value) throws IOException {

		byte[] BigFile = new byte[50];
			
		fileSys = split.getPath().getFileSystem(job);
		fsStreamBigFile = fileSys.open(split.getPath());
		
		patt = scan.nextLine();

		key.set(FileName); //setfilename as a key
				
		fsStreamBigFile.skip(offset);
		fsStreamBigFile.read(BigFile, offset, patt.length());

		value.setLoInteger(offset);
		value.setBigFile(BigFile.toString());
		value.setPatString(patt);

		// increment the offset
		offset += patt.length();

		
		if (!scan.hasNext()) {
			return false;
		} else {
			return true;
		}
	}

	public Text createKey() {
		// TODO Auto-generated method stub
		//testkuy
		return new Text();
	}

	public PartialString createValue() {
		// TODO Auto-generated method stub
		return new PartialString(patt, offset);
	}

	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return offset;
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsStreamPatt.close();
		fsStreamBigFile.close();
		scan.close();
	}

	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return offset;
	}

}
