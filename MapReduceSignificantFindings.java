import org.apache.commons.math3.distribution.BetaDistribution;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceSignificantFindings extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length != 6) {
			System.out.println("\n" + 
					"This program runs a mapreduce to determine the p-value entries that are significant at the FDR cutoff \n" +  
					"Usage is: \n\n" +
					"hadoop jar [jarFile] MapReduceSignificantFindings [args0] [args1] [args2] [args3] [args4] [args5] \n\n" + 
					"args0 - input path of p-values \n" +
					"args1 - output path to significant findings \n" +
					"args2 - pi0 proportion of null hypotheses \n" +
					"args3 - alpha for the beta distribution for the true hypotheses \n" +
					"args4 - beta for the beta distribution for the true hypotheses \n" +
					"args5 - false discovery rate cutoff for significance \n"
					);
			return;
		}
		int res = ToolRunner.run(new Configuration(), new MapReduceSignificantFindings(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		
		// the q-value cut-off is the highest tolerated false discovery rate for significance (or 1 - the Bayesian likelihood of being a true discovery)
		conf.set("significanceQValueCutOff", args[5]);
		conf.set("pi0", args[2]);
		conf.set("alpha", args[3]);
		conf.set("beta", args[4]);

		Job job = Job.getInstance(conf, "");
		job.setJarByClass(MapReduceCDFFalseDiscoveryRate.class);

		job.setJobName("calcBUM");
		
		job.setMapperClass(CheckSignificanceMapper.class);
	
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class CheckSignificanceMapper extends Mapper<Object, Text, NullWritable, Text> {

		private double signficancePValueCutoff = 0;

		public void setup(Context context) throws IOException, InterruptedException {

			double pi0 = Double.parseDouble(context.getConfiguration().get("pi0"));
			double alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
			double beta = Double.parseDouble(context.getConfiguration().get("beta"));

			findSignficancePValueCutoff(pi0,alpha,beta,Double.parseDouble(context.getConfiguration().get("significanceQValueCutOff")));
		}

		public void findSignficancePValueCutoff(double pi0, double alpha, double beta, double significanceQValueCutoff) {

			if ((1-pi0) <= significanceQValueCutoff) {
				signficancePValueCutoff = 1;
				return;
			}

			BetaDistribution trueDiscoveries = new BetaDistribution(alpha, beta);
			
			double upperBound = 1;
			double lowerBound = 0;
			double boundTolerance = Math.pow(10,-4);

			double significantPValueCutoffGuess;
			double fractionDifferenceInBounds;
			do {
				significantPValueCutoffGuess = (lowerBound + upperBound) / 2;
				if (determineQValue(pi0,trueDiscoveries,significantPValueCutoffGuess) <= significanceQValueCutoff) {
					lowerBound = significantPValueCutoffGuess;
				} else {
					upperBound = significantPValueCutoffGuess;
				}
				fractionDifferenceInBounds = (upperBound - lowerBound) / upperBound;
			} while (fractionDifferenceInBounds > boundTolerance);

			signficancePValueCutoff = significantPValueCutoffGuess;
		}

		public double determineQValue(double pi0, BetaDistribution trueDiscoveries, double significantPValueCutoffGuess) {

			double portionFalseDiscoveries = pi0 * significantPValueCutoffGuess;
			double portionTrueDiscoveries = (1-pi0) * trueDiscoveries.cumulativeProbability(significantPValueCutoffGuess);

			return portionFalseDiscoveries / (portionFalseDiscoveries + portionTrueDiscoveries);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			double pValue = transformXmlToPValues(value.toString()).doubleValue();

			if (pValue <= signficancePValueCutoff) {
				context.write(NullWritable.get(), value);
			}
		}

		public static Double transformXmlToPValues(String xml) {

			String startDelim = "p=\"";
			String stopDelim = "\" t=";
			int startIndex = 0; 
			int stopIndex = 0;

			startIndex = xml.indexOf(startDelim,stopIndex);
			startIndex += startDelim.length();
			stopIndex = xml.indexOf(stopDelim,startIndex);

			return Double.parseDouble(xml.substring(startIndex,stopIndex));
		}
	}
}
