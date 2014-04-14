import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PositionInputFormat extends FileInputFormat<Text, Text> {

	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	private static final double SPLIT_SLOP = 1.1;
	

	@Override
	public org.apache.hadoop.mapreduce.RecordReader<Text, Text> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new PosRecordReader(split, context);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// TODO Auto-generated method stub
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();
			long length = file.getLen();
			if (length != 0) {
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus) {
					blkLocations = ((LocatedFileStatus) file)
							.getBlockLocations();
				} else {
					FileSystem fs = path.getFileSystem(job.getConfiguration());
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path)) {
					long blockSize = file.getBlockSize();
					// long splitSize = computeSplitSize(blockSize, minSize,
					// maxSize);
					long splitSize = 4;

					long bytesRemaining = length;
					while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining,
								splitSize, blkLocations[blkIndex].getHosts()));
						bytesRemaining -= splitSize;
					}

					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining,
								bytesRemaining,
								blkLocations[blkIndex].getHosts()));
					}
				} else { // not splitable
					splits.add(makeSplit(path, 0, length,
							blkLocations[0].getHosts()));
				}
			} else {
				// Create empty hosts array for zero length files
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}

}
