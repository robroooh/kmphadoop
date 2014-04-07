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
		System.out.println("reduce get ");
		System.out.println(key.toString());
		StringBuilder sb = new StringBuilder();
		Iterator<Text> ite = it.iterator();

		while (ite.hasNext()) {
			sb.append(ite.next().toString());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);

		context.write(key, new Text(sb.toString()));
	}
}
