import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ReduceClass implements Reducer<K2, V2, K3, V3> {

	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void reduce(K2 key, Iterator<V2> values,
			OutputCollector<K3, V3> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
