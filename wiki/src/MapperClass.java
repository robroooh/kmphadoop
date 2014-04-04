import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Text, PartialString, Text, Text> {

	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text key, PartialString value, Context context)
			throws IOException, InterruptedException {

		if (value.getBigFile().equals(value.getPatString())) {
			context.write(
					new Text(key.toString() + "," + value.getPatString()),
					new Text(value.getLoInteger().toString()));
		}

	}
}
