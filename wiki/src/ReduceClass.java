import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 public class ReduceClass
extends Reducer<Text, Text, Text, Text> {
	 
	public void reduce(Text key, Iterator<Text> it,
			Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		System.out.println("REDUCE GET THIS SHIT");
		System.out.println(key.toString()+","+it.toString());
		while (it.hasNext()) {
			sb.append(it.next().toString());
			sb.append(",");
			System.out.println(sb.toString());
		}
		context.write(key, new Text(sb.toString()));
	}
}


