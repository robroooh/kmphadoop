import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
	private int EOF;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		patt = new ArrayList<String>();// Ohhh this is an array list
		this.split = split; // Ohhh thi \s is a split
		this.context = context; // Dafaq is context thing,, shittttttttt pooooo
								// peee !!
		try {
			URI[] cache = context.getCacheFiles();

			FileInputStream fis = new FileInputStream(cache[0].getPath());
			scan = new Scanner(fis);
			while (scan.hasNext()) {
				patt.add(scan.nextLine());
			}
			index = 0;
			offset = 0;

			filePath = ((FileSplit) split).getPath();

			fSystem = filePath.getFileSystem(context.getConfiguration());
			fsBigFile = fSystem.open(filePath);
			System.out.println("DONE GETTING POSRECORDREADER CONSTRUCTOR");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Cache File not found");
		}
	}

	/**
	 * @param split
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public PosRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new PartialString();
		}

		if (index < patt.size()) {
			buffer = new byte[patt.get(index).length()];

			EOF = fsBigFile.read(buffer, 0, patt.get(index).length());

			key.set(filePath.getName());

			value.setPatString(patt.get(index));
			value.setLoInteger(offset);
			value.setBigFile(new String(buffer));

			System.out.println("Sending BIg File : " + new String(buffer));
			System.out.println("Sending Pattern : " + value.getPatString());
			System.out.println("EOF is " + EOF);
			
			offset += EOF;
			
			if (EOF == patt.get(index).length()) {
				fsBigFile.seek(offset);
				buffer = null;
			} else {
				fsBigFile.seek(0);
				offset = 0;
				index++;
				EOF = 0;
			}
			return true;
		} else {
			System.out.println("Current index: " + index + "pat size"
					+ patt.size());
			System.out.println("Byeee this is my last key,value");
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
		return 0;
	}

	@Override
	public void close() throws IOException {
		scan.close();
	}

}
