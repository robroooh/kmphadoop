import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

 public class ReduceClass
extends Reducer<Text, Text, Text, Text> {
	 
	public void reduce(Text key, Iterator<Text> it,
			Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			sb.append(it.next().toString());
			sb.append(",");
		}
		context.write(key, new Text(sb.toString()));
	}
}


