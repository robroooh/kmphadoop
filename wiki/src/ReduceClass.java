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
		// System.out.println("The reduce is called");

		Iterator<Text> ite = it.iterator();
		String prefix = "";

		for (Text text : it) {
			sb.append(prefix);
			prefix = ",";
			sb.append(text.toString());
		}
		context.write(new Text(key.toString().trim()), new Text(sb.toString()));
	}
}
