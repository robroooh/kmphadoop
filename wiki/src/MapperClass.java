import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperClass extends Mapper<Text, PartialString, Text, Text> {

	@Override
	protected void map(Text key, PartialString value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		if (value.getBigFile().equals(value.getPatString())) {

			Text test = new Text(value.toString());

			context.write(key, test);

			System.out.println(key.toString() + ": " + value.toString());

		}

	}
}
