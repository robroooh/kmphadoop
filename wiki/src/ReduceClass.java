import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> it, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		Iterator<Text> ite = it.iterator();

		while (ite.hasNext()) {
			sb.append(ite.next());
			if (sb.length() > 1536000) {
				sb.deleteCharAt(sb.length() - 1);
				context.write(key, new Text(sb.toString()));
				sb.setLength(0);
				sb.trimToSize();
			}
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
			context.write(key, new Text(sb.toString()));
		}
		else{
			context.write(key, new Text());
		}
		
		

	}
}
