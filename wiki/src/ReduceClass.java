import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

public class ReduceClass extends org.apache.hadoop.mapreduce.Reducer<Text, PartialString, Text, String> {

	protected void reduce(Text key, Iterable<PartialString> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
			context.write(key, values.toString());
	
	}


}
