import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperClass extends Mapper<Text, PartialString, Text, Text> {

	@SuppressWarnings("unchecked")
	@Override
	protected void map(Text key, PartialString value,
			Context context)
			throws IOException, InterruptedException {
	/*	
		System.out.println("Map get the following :");
		System.out.println("Key = " + key.toString());
		System.out.println("Pattern = " + value.getPatString());
		System.out.println("Offset = " + value.getLoInteger());
		System.out.println("BigFile = " + value.getBigFile());
		System.out.println("---------");
		*/
		if (value.getBigFile().equals(value.getPatString())) {
			context.write(new Text(key.toString()+","+value.getPatString()), new Text(value.getLoInteger().toString()));
		}

	}
}
