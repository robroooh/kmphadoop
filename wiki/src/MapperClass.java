import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapperClass extends Mapper<LongWritable, Text, Text, NullWritable>{
	private Text out = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/*
		out.set(key.toString()+value.toString());
		context.write(out, NullWritable.get());
		*/
	}

	

}
