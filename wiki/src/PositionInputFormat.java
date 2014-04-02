import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.StringUtils;

public class PositionInputFormat extends FileInputFormat<Text, PartialString> {

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<Text, PartialString> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		FileSystem fs;

		context.getConfiguration().getConfResourceAsInputStream(null);
		
		return new PosRecordReader(split,context);
	}

}
