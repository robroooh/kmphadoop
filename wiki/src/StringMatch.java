import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

public class StringMatch {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
		}

		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PartialString.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(PositionInputFormat.class);
		job.setOutputFormatClass(LazyOutputFormat.class);

		try {
			job.addCacheFile(new URI("./pattern.txt"));
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			System.out.println("HOLY FUCKING COW GOD, NO PATTERN FILE IS FOUND");
		}
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.submit();
	}
}
