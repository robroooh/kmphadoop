import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 public class ReduceClass
extends Reducer<Text, Text, Text, Text> {
	 
	public void reduce (Text key, Iterable<Text> it,
			Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		System.out.println("The reduce is called");
		Iterator<Text> ite = it.iterator();
		while (ite.hasNext()) {
			sb.append(ite.next().toString());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		context.write(key, new Text(sb.toString()));
	}
}


