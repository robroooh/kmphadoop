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
		System.out.println("The reduce is called");

		Iterator<Text> ite = it.iterator();
		ArrayList<Long> inte = new ArrayList<Long>();
		String[] values;
		values = ite.toString().split(",");
		
		for (String string : values) {
			inte.add(Long.parseLong(string));
		}
		Collections.sort(inte);
		
		for (Long integer : inte) {
			sb.append(integer.toString());
			sb.append(",");	
		}
	
		sb.deleteCharAt(sb.length() - 1);

		context.write(key, new Text(sb.toString()));
	}
}
