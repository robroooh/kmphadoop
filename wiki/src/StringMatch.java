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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class StringMatch {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		JobConf jobc = new JobConf(StringMatch.class);
		jobc.setJobName("StringMatching");
		
		jobc.setMapOutputKeyClass(Text.class);
		jobc.setMapOutputValueClass(PartialString.class);

		jobc.setMapperClass(MapperClass.class);
		jobc.setReducerClass(ReduceClass.class);

		jobc.setInputFormat(PositionInputFormat.class);
		jobc.setOutputFormat(TextOutputFormat.class);

        try {
			DistributedCache.addCacheFile(new URI("./stringlist.txt"), jobc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error from distributedcache in Main");
		}

        FileInputFormat.setInputPaths(jobc, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobc, new Path(args[1]));

        JobClient.runJob(jobc);
	}
}
