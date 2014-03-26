import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperClass extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<Text, PartialString, Text, Text> {

	public void map(Text key, PartialString value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		if (value.getBigFile().equals(value.getPatString())) {

			Text test = new Text(value.toString());

			output.collect(key, test);
			System.out.println(key.toString() + ": " + value.toString());
		}
	}

}
