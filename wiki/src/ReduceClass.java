import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceClass extends MapReduceBase implements
		Reducer<Text, PartialString, Text, PartialString> {

	public void configure(JobConf job) {
		// TODO Auto-generated method stub

	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	public void reduce(Text key, Iterator<PartialString> values,
			OutputCollector<Text, PartialString> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
