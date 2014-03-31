import java.io.BufferedInputStream;
import java.io.BufferedReader;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

public class PosRecordReader implements RecordReader<Text, PartialString> {

	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	private Integer offset = 0;
	private FileSystem fileSys;
	private FSDataInputStream fsStreamBigFile;
	private FSDataInputStream fsStreamPatt;
	private String patt;
	private String FileName;

	/**
	 * @param lineReader
	 * @param lineKey
	 * @param lineValue
	 */
	public PosRecordReader(JobConf job, FileSplit split) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path pattfile = new Path("hdfs://pattern.txt");
		if (!fs.exists(pattfile)) {
			System.out.println("input file not gound");
		}

		fsStreamPatt = fs.open(pattfile);

		lineReader = new LineRecordReader(job, split);

		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();

		fileSys = split.getPath().getFileSystem(job);

		fsStreamBigFile = fileSys.open(split.getPath());
		FileName = split.getPath().getName();

	}

	public boolean next(Text key, PartialString value) throws IOException {
		// TODO Auto-generated method stub
		patt = fsStreamPatt.readLine();
		byte[] BigFile = new byte[50];
		// / set here, key is filename, value contains three string, string that
		// read from bigfile, patternString, offset
		key.set(FileName);
		fsStreamBigFile.skip(offset);
		fsStreamBigFile.read(BigFile, offset, patt.length());

		value.setLoInteger(offset);
		value.setBigFile(BigFile.toString());
		value.setPatString(patt);

		// increment the offset
		offset += patt.length();

		if (fsStreamPatt.read() == -1) {
			return false;
		} else {
			return true;
		}
	}

	public Text createKey() {
		// TODO Auto-generated method stub
		// testkuy
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
	}

	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return offset;
	}

}
