import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

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
	private FSDataInputStream fsStream;
	private Scanner scan;
	private String patt;
	private String FileName;

	/**
	 * @param lineReader
	 * @param lineKey
	 * @param lineValue
	 */
	public PosRecordReader(JobConf job, FileSplit split) throws IOException {
		lineReader = new LineRecordReader(job, split);

		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();

		fileSys = split.getPath().getFileSystem(job);

		fsStream = fileSys.open(split.getPath());
		FileName = split.getPath().getName();

		scan = new Scanner(job.getResource("pattern").openStream());
	}

	public boolean next(Text key, PartialString value) throws IOException {
		// TODO Auto-generated method stub
		patt = scan.nextLine();
		byte[] BigFile = new byte[50];
		// / set here, key is filename, value contains three string, string that
		// read from bigfile, patternString, offset
		key.set(FileName);
		fsStream.skip(offset);
		fsStream.read(BigFile, offset, patt.length());

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
		scan.close();
		fsStream.close();
	}

	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return offset;
	}

}
