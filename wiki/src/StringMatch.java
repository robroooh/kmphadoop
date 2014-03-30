import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;


public class StringMatch {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		JobConf jobc = new JobConf();
		
		jobc.setInputFormat(PositionInputFormat.class);
		jobc.setMapperClass((Class<? extends Mapper>) MapperClass.class);
			
		FileInputFormat.addInputPath(jobc, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobc, new Path(args[1]));
		
	    Job job = Job.getInstance(jobc);
	}
}
