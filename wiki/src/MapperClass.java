import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Text, Text, Text, Text> {

	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		//System.out.println("map is called");
		System.out.println("Map get");
		System.out.println(key.toString());
		System.out.println(value.toString());
		context.write(key, value);

	}
}
