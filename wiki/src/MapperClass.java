import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.omg.CORBA.PRIVATE_MEMBER;

public class MapperClass extends Mapper<Text, Text, Text, Text> {

	private ArrayList<String> patt;
	private ArrayList<Long> loInteger;
	private Scanner scan;
	private Context context;
	private long splitStart;

	private static final Integer SPLIT_LENGTH = 33550000 + 99;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		this.context = context;
		patt = new ArrayList<String>();

		URI[] cache = context.getCacheFiles();

		FileInputStream fis = new FileInputStream(cache[0].getPath());
		scan = new Scanner(fis);

		while (scan.hasNext()) {
			patt.add(scan.nextLine());
		}
		splitStart = ((FileSplit)context.getInputSplit()).getStart();

	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		for (int index = 0; index < patt.size(); index++) {

			searchSubString(value.toString().toCharArray(), patt.get(index)
					.toCharArray());

			if (loInteger.size() > 0) {
				for (int i = 0; i < loInteger.size(); i++) {
					context.write(
							new Text(key.toString() + "," + patt.get(index)),
							new Text(loInteger.get(i).toString() + ","));

				}
			} else {
				context.write(new Text(key.toString() + "," + patt.get(index)),
						new Text());
			}

		}
	}

	public int[] preProcessPattern(char[] ptrn) {
		int i = 0, j = -1;
		int ptrnLen = ptrn.length;
		int[] b = new int[ptrnLen + 1];

		b[i] = j;
		while (i < ptrnLen) {
			while (j >= 0 && ptrn[i] != ptrn[j]) {
				// if there is mismatch consider next widest border
				j = b[j];
			}
			i++;
			j++;
			b[i] = j;
		}
		return b;
	}

	public void searchSubString(char[] text, char[] ptrn) {
		loInteger = new ArrayList<Long>();
		text = Arrays.copyOf(text, SPLIT_LENGTH - 99 + ptrn.length - 1);

		int i = 0, j = 0;
		int ptrnLen = ptrn.length;
		int txtLen = text.length;
		int[] b = preProcessPattern(ptrn);

		while (i < txtLen) {
			while (j >= 0 && text[i] != ptrn[j]) {
				j = b[j];
			}
			i++;
			j++;

			if (j == ptrnLen) { //match occurs here 
				loInteger.add(splitStart + (i - ptrnLen));
			
				j = b[j];
			}
		}
	}
}