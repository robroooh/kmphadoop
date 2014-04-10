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
		boolean flag = false;

		while (ite.hasNext()) {
			if (flag != false) {
				sb.append(",");
			}
			sb.append(ite.next());
			flag = false;

			if (sb.length() > 1536000) {
				context.write(key, new Text(sb.toString()));
				sb.setLength(0);
				sb.trimToSize();
			}
		}
		context.write(key, new Text(sb.toString()));

	}
}
