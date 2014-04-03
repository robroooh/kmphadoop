import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PosRecordReader extends
		org.apache.hadoop.mapreduce.RecordReader<Text, PartialString> {
	private InputSplit split;
	private TaskAttemptContext context;
	private Scanner scan;
	private ArrayList<String> patt;
	private int index;
	private int offset;
	private Path filePath;
	private byte[] buffer;
	private FileSystem fSystem;
	private FSDataInputStream fsBigFile;
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
	 * @throws IOException 
	 */
	public PosRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		super();
		patt = new ArrayList<String>();// Ohhh this is an array list
		this.split = split; // Ohhh thi \s is a split
		this.context = context; // Dafaq is context thing,, shittttttttt pooooo
								// peee !!
		try {
			URI[] cache = context.getCacheFiles();

			scan = new Scanner(cache[0].getPath());
			while (scan.hasNext()) {
				patt.add(scan.nextLine());
			}
			index = 0;
			offset = 0;
			
			filePath = ((FileSplit) split).getPath();

		fSystem = filePath.getFileSystem(context.getConfiguration());
		fsBigFile = fSystem.open(filePath);
		key = new Text();
		value = new PartialString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Cache File not found");
		}
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (index<patt.size()) {
			buffer = new byte[patt.get(index).length()];

			fsBigFile.read(buffer, offset, patt.get(index).length());

			key.set(filePath.getName());

			value.setPatString(patt.get(index));
			value.setLoInteger(offset);
			value.setBigFile(buffer.toString());
			
			offset += patt.get(index).length();
			index++;
			buffer = null;
			
			return true;
		} else {
			return false;
		}
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public PartialString getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return split.getLength() / offset;
	}

	@Override
	public void close() throws IOException {
		scan.close();
	}

}
