import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PosRecordReader extends org.apache.hadoop.mapreduce.RecordReader<Text, PartialString> {
	private InputSplit split;
	private TaskAttemptContext context;
	private Scanner scan;
	private ArrayList<String> patt;
	private int	index;
	private int offset;
	
	private Text key;
	private PartialString value;

		
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = split;
		this.context = context;
	}
	
	/**
	 * @param split
	 * @param context
	 */
	public PosRecordReader(InputSplit split, TaskAttemptContext context) {
		super();
		patt = new ArrayList<String>();
		this.split = split;
		this.context = context;
		try {
			URI[] cache = context.getCacheFiles();
			scan = new Scanner(cache[0].toURL().openStream());
			while(scan.hasNext()){
				patt.add(scan.nextLine());
			}
			index = 0;
			offset = 0;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Cache File not found");
		}
		
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		byte[]	buffer = new byte[patt.get(index).length()];

		Path filePath = ((FileSplit)split).getPath();
		FileSystem fSystem;

		fSystem = filePath.getFileSystem(context.getConfiguration());
		FSDataInputStream fsBigFile = fSystem.open(filePath);
		
		fsBigFile.read(buffer, offset, patt.get(index).length());

		key.set(fSystem.getName());
		
		value.setPatString(patt.get(index));
		value.setLoInteger(offset);
		value.setBigFile(buffer.toString());
		
		index++;
		offset += patt.get(index).length();
		
		return true;
		// TODO Auto-generated method stub	
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PartialString getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
