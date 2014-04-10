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
	private Path filePath;
	private byte[] buffer;
	private FileSystem fSystem;
	private FSDataInputStream fsBigFile;
	private Text key;
	private Text value;
	private int flag = 0;

	private static final Integer SPLIT_LENGTH = 15360000;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = split;
		this.context = context;

		filePath = ((FileSplit) split).getPath();

		fSystem = filePath.getFileSystem(context.getConfiguration());
		fsBigFile = fSystem.open(filePath);
		fsBigFile.seek(((FileSplit) split).getStart());

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
		if (flag == 0) {

			buffer = new byte[SPLIT_LENGTH];

			if (fsBigFile.available() < SPLIT_LENGTH) {
				fsBigFile.readFully(buffer, 0, fsBigFile.available());
			} else {
				fsBigFile.readFully(buffer, 0, SPLIT_LENGTH);
			}
			key.set(filePath.getName());

			value.set(buffer);

			flag = 1;
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
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}

}
