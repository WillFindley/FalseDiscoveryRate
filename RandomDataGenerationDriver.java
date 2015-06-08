import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

public class RandomDataGenerationDriver{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configurations();

		int numMapTasks = Integer.parseInt(arg[0]);
		int numRecordsPerTask = Integer.parseInt(arg[1]);
		Path wordList = new Path(args[2]);
		Path outputDir = new Path(args[3]);

		Job job = new Job(conf, "RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerationDriver.class);
		
		// Because we're mapping out random data and not analyzing
		job.setNumReduceTasks(0);

		job.setInputFormatClass(RandomStackOverflowInputFormat.class);

		RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
		RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
		RandomStackOverflowInputFormat.setRandomWordList(job, wordList);

		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
