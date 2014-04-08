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
		org.apache.hadoop.mapreduce.RecordReader<Text, Text> {
	private InputSplit split;
	private TaskAttemptContext context;
	private Scanner scan;
	private ArrayList<String> patt;
	private Integer index;
	private Path filePath;
	private byte[] buffer;
	private FileSystem fSystem;
	private FSDataInputStream fsBigFile;
	private Text key;
	private Text value;
	private Long EOF;
	private StringBuilder loInteger;
	private static final Integer SPLIT_LENGTH = 15360000;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		patt = new ArrayList<String>();
		this.split = split;
		this.context = context;

		try {
			URI[] cache = context.getCacheFiles();

			FileInputStream fis = new FileInputStream(cache[0].getPath());
			scan = new Scanner(fis);
			while (scan.hasNext()) {
				patt.add(scan.nextLine());
			}
			index = 0;

			filePath = ((FileSplit) split).getPath();

			fSystem = filePath.getFileSystem(context.getConfiguration());
			fsBigFile = fSystem.open(filePath);
			fsBigFile.seek(((FileSplit) split).getStart());
			/*
			 * System.out .println("get start =  " + ((FileSplit)
			 * split).getStart());
			 */
			loInteger = new StringBuilder();
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
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}

		if (index < patt.size()) {

			buffer = new byte[SPLIT_LENGTH];

			// modify how long to read here
			if (fsBigFile.available() < SPLIT_LENGTH) {
				fsBigFile.readFully(buffer, 0, fsBigFile.available());
			} else {
				fsBigFile.readFully(buffer, 0, SPLIT_LENGTH);
			}
			key.set(filePath.getName() + "," + patt.get(index));
			String s = new String(buffer);

			searchSubString(s.toCharArray(), patt.get(index).toCharArray());

			value.set(loInteger.toString());
		
			// if reach to EOF
			if (fsBigFile.available() == 0) {
				buffer = null;

				index++;

				fsBigFile.seek(((FileSplit) split).getStart());

				loInteger.setLength(0);
				loInteger.trimToSize();
				return true;
			} else {
				buffer = null;

				index++;
				
				loInteger.setLength(0);
				loInteger.trimToSize();
				return true;
			}

		} else {
			return false;
		}
		/*
		 * if (EOF == patt.get(index).length()) { fsBigFile.seek(offset); buffer
		 * = null; } else if (lenOfSplit == ((FileSplit) split).getLength() ||
		 * EOF == -1) { fsBigFile.seek(0); offset = 0; index++; EOF = 0;
		 * lenOfSplit = 0; } return true; } else { return false; }
		 */

	}

	public int[] preProcessPattern(char[] ptrn) {
		int i = 0, j = -1;
		int ptrnLen = ptrn.length;
		int[] b = new int[ptrnLen + 1];

		b[i] = j;
		while (i < ptrnLen) {
			while (j >= 0 && ptrn[i] != ptrn[j]) {
				// if there is mismatch consider next widest border
				j = b[j];
			}
			i++;
			j++;
			b[i] = j;
		}
		return b;
	}

	public void searchSubString(char[] text, char[] ptrn) {
		int i = 0, j = 0;
		// pattern and text lengths
		int ptrnLen = ptrn.length;
		int txtLen = text.length;
		// initialize new array and preprocess the pattern
		int[] b = preProcessPattern(ptrn);

		while (i < txtLen) {
			while (j >= 0 && text[i] != ptrn[j]) {
				j = b[j];
			}
			i++;
			j++;

			// a match is found
			if (j == ptrnLen) {
				System.out.println("LOinteger = " + (i - ptrnLen));
				loInteger.append((String.valueOf(i - ptrnLen)) + ",");
				j = b[j];
			}
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
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
