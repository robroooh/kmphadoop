import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PositionInputFormat extends FileInputFormat<Text, Text> {

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<Text, Text> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new PosRecordReader(split, context);
	}

}
