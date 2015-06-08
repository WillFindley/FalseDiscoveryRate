import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable; 

public class RandomDataGenerationDriver{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
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

	// There's not much in here other than empty overrides because we're not reading in anything, just writing
	public static class FakeInputSplit extends InputSplit implements Writable {

		public void readFields(DataInput arg0) throws IOException {
		}

		public void write(DataOutput arg0) throws IOException {
		}

		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}
	}

	public static class RandomStackOverflowInputFormat extends InputFormat<Text,NullWritable> {
		
		public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
		public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
		public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

		public List<InputSplit> getSplits(JobContext job) throws IOException {

			int numSplits = job.getConfiguration().getInt(NUM_MAPS_TASKS, -1);

			ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			for (int i = 0; i < numSplits; i++) {
				splits.add(new FakeInputSplit());
			}

			return splits;
		}
}
