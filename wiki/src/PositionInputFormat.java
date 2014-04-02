import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import javax.swing.plaf.multi.MultiInternalFrameUI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.StringUtils;

public class PositionInputFormat extends FileInputFormat<Text, PartialString> {

	private ArrayList<Integer> patSize = new ArrayList<Integer>();
	
	
	@Override
	public org.apache.hadoop.mapreduce.RecordReader<Text, PartialString> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		FileSystem fs;

		context.getConfiguration().getConfResourceAsInputStream(null);
		
		return new PosRecordReader(split,context);
	}
	/*public void FetchPatSize(TaskAttemptContext context){
		try {
			URI[] filelists = context.getCacheFiles();
			File query = new File(filelists[0].getPath());
			
			Scanner scan = new Scanner(query);
			while (scan.hasNext()) {
				patSize.add(scan.nextLine().length());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}*/

}
