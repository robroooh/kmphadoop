import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class PositionInputFormat extends FileInputFormat<Text,PartialString>{


	@Override
	public RecordReader<Text, PartialString> getRecordReader(
			InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		reporter.setStatus(split.toString());
		
		return new PosRecordReader(job, (FileSplit) split);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}

}