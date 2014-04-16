import java.awt.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<BytesWritable, BytesWritable, Text, Text> {

	public static final String charset = "UTF-8";
	
	public void reduce(BytesWritable key, Iterable<BytesWritable> it, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		Iterator<BytesWritable> ite = it.iterator();

		while (ite.hasNext()) {
			
			sb.append(new String(ite.next().copyBytes(), charset));
			
			if (sb.length() > 1536000) {
				sb.deleteCharAt(sb.length() - 1);
				context.write(new Text(new String(key.copyBytes(), charset)), new Text(sb.toString()));
				sb.setLength(0);
				sb.trimToSize();
			}
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
			context.write(new Text(new String(key.copyBytes(), charset)), new Text(sb.toString()));
		}
		else{
			context.write(new Text(new String(key.copyBytes(), charset)), new Text());
		}
		
		

	}
}
