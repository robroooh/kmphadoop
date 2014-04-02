import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperClass extends Mapper<Text, PartialString, Text, Text> {

	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text key, PartialString value,
			Context context)
			throws IOException, InterruptedException {

		if (value.getBigFile().equals(value.getPatString())) {

			Text test = new Text();
			test.set(key.toString()+","+value.getPatString());
			
			context.write(new Text(test), new Text(value.getLoInteger().toString()));

			System.out.println(key.toString() + ": " + value.getLoInteger());

		}

	}
}
