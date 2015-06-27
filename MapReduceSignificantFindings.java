import org.apache.commons.math3.distribution.BetaDistribution;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.ArrayList;
import java.io.IOException;

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

		if (args.length != 4) {
			System.out.println("\n" + 
					"This program runs a mapreduce to determine the coefficients for a beta-uniform model of the p-value CDF \n" +  
					"Usage is: \n\n" +
					"hadoop jar [jarFile] MapReduceSignificantFindings [args0] [args1] [args2] [args3] \n\n" + 
					"args0 - input path of p-values \n" +
					"args1 - path to BUM coefficients \n" +
					"args2 - output path to significant findings \n" +
					"args3 - false discovery rate cutoff for significance \n"
					);
			return;
		}
		int res = ToolRunner.run(new Configuration(), new MapReduceSignificantFindings(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		
		// the q-value cut-off is the highest tolerated false discovery rate for significance (or 1 - the Bayesian likelihood of being a true discovery)
		conf.set("significanceQValueCutOff", args[3]);
		conf.set("pathToBUMCoefficients", args[1]);

		Job job = Job.getInstance(conf, "");
		job.setJarByClass(MapReduceCDFFalseDiscoveryRate.class);

		job.setJobName("calcBUM");
		
		job.setMapperClass(CheckSignificanceMapper.class);
		job.setReducerClass(FDRModelAveragingReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pi0AlphaBetaCountTuple.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Pi0AlphaBetaCountTuple.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class CheckSignificanceMapper extends Mapper<Object, Text, Text, Pi0AlphaBetaCountTuple> {

		private double signficancePValueCutoff = 0;

		public void setup(Context context) throws IOException, InterruptedException {

			try {
				Path pt=new Path("hdfs:" + context.getConfiguration().get("pathToBUMCoefficients") + "/part-r-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line = br.readLine();
			} catch (IOException e) {
				System.err.println("File opening failed:");
				e.printStackTrace();
			}

		        String startDelim = "pi0: ";
			String stopDelim = "\talpha";
			int startIndex = 0;
			int stopIndex = 0;
	
			startIndex = line.indexOf(startDelim,stopIndex);
			startIndex += startDelim.length();
			stopIndex = line.indexOf(stopDelim,startIndex);

			double pi0 = Double.parseDouble(line.substring(startIndex,stopIndex));


		        String startDelim = "alpha: ";
			String stopDelim = "\tbeta";
			int startIndex = 0;
			int stopIndex = 0;
	
			startIndex = line.indexOf(startDelim,stopIndex);
			startIndex += startDelim.length();
			stopIndex = line.indexOf(stopDelim,startIndex);

			double alpha = Double.parseDouble(line.substring(startIndex,stopIndex));


		        String startDelim = "beta: ";
			String stopDelim = "\tcount";
			int startIndex = 0;
			int stopIndex = 0;
	
			startIndex = line.indexOf(startDelim,stopIndex);
			startIndex += startDelim.length();
			stopIndex = line.indexOf(stopDelim,startIndex);

			double beta = Double.parseDouble(line.substring(startIndex,stopIndex));

			
			signficancePValueCutoff = findSignficancePValueCutoff(pi0,alpha,beta,Double.parseDouble(context.getConfiguration().get("significanceQValueCutOff")));
		}

		public double findSignficancePValueCutoff(double pi0, double alpha, double beta, double significanceQValueCutoff) {

			if ((1-pi0) <= significanceQValueCutoff) {
				return 1;
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
			while (fractionDifferenceInBounds > boundTolerance);

			return significantPValueCutoffGuess;
		}

		public double determineQValue(double pi0, BetaDistribution trueDiscoveries, double significantPValueCutoffGuess) {

			double portionFalseDiscoveries = pi0 * significantPValueCutoffGuess;
			double portionTrueDiscoveries = (1-pi0) * trueDiscoveries.cumulativeProbability(significantPValueCutoffGuess);

			return portionFalseDiscoveries / (portionFalseDiscoveries + portionTrueDiscoveries);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			tmpPValues.add(transformXmlToPValues(value.toString()));

			int numSamplesForFit = Integer.parseInt(context.getConfiguration().get("numSamplesForFit"));
			if (tmpPValues.size() == numSamplesForFit) {

				double[][] pValues = calculateEmpiricalCDF(tmpPValues.toArray(new Double[numSamplesForFit]));
				tmpPValues = new ArrayList<Double>();

				double[] coeffs = getOptCoeffs(pValues);

				coeffAns.setPi0(coeffs[0]);
				coeffAns.setAlpha(coeffs[1]);
				coeffAns.setBeta(coeffs[2]);
				coeffAns.setCount(1);

				// BUM = Beta-Uniform Distribution
				context.write(allContribute, coeffAns);
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

		private double[][] calculateEmpiricalCDF(Double[] tmpPValues) {

			double[][] pValues = new double[tmpPValues.length][2];
			for (int i = 0; i < tmpPValues.length; i++) {
				pValues[i][0] = tmpPValues[i].doubleValue();
			}

			// sort on p-values so that second column can be filled with empirical CDF values
			Arrays.sort(pValues, new Comparator<double[]>() {
				@Override
				public int compare(double[] entry1, double[] entry2) {
					return Double.compare(entry1[0],entry2[0]);
				}
			});

			// calculate emprical CDF values
			for (int i = 1; i <= pValues.length; i++) {
				pValues[i-1][1] = i / (double) pValues.length;
			}

			return pValues;
		}

		private double[] getOptCoeffs(double[][] pValues) {	

			Random rndm = new Random();

			// pi0 is 1 because should always conservatively start by overestimating the proportion of negatives	
			double[] coeffs = {1.0, rndm.nextDouble(), 1+rndm.nextInt(9)+rndm.nextDouble()};
			double avDelta = 1;
			double oldDelta = avDelta;
			double learningRate = 2;

			int sigDigits = 4; // number of significant digits in the model parameters;
			double tolerance = 1.0 / Math.pow(10,sigDigits);
			do {
				oldDelta = avDelta;
				// coeffs is implicitly returned because it is modified at the reference position
				avDelta = stochasticGradientDescent(pValues, coeffs, tolerance, learningRate);
				if (oldDelta < avDelta) learningRate *= 0.9;
			} while (avDelta >= tolerance);

			return coeffs;	
		}

		private double stochasticGradientDescent(double[][] pValues, double[] coeffs, double gradientStepSize, double learningRate) {

			pValues = shuffleEmpiricalCDF(pValues);

			double avDelta = 0;
			double momentum = 0.5;
			double deltaPi0 = 0;
			double deltaAlpha = 0;
			double deltaBeta = 0;
			for (double[] data : pValues) {

				deltaPi0 = momentum * deltaPi0 + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] + (gradientStepSize/2),coeffs[1],coeffs[2]),2) - 
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0] - (gradientStepSize/2),coeffs[1],coeffs[2]),2)) / gradientStepSize;
				deltaAlpha = momentum * deltaAlpha + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] + (gradientStepSize/2),coeffs[2]),2) -
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1] - (gradientStepSize/2),coeffs[2]),2)) / gradientStepSize;
				deltaBeta = momentum * deltaBeta + (1-momentum) * learningRate *
					(Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] + (gradientStepSize/2)),2) -
					 Math.pow(data[1] - calcModelCDFValue(data[0], coeffs[0],coeffs[1],coeffs[2] - (gradientStepSize/2)),2)) / gradientStepSize;
				avDelta = (avDelta + Math.sqrt(Math.pow(deltaPi0,2) + Math.pow(deltaAlpha,2) + Math.pow(deltaBeta,2)))/2;

				System.out.println("Current average error: " + avDelta);

				coeffs[0] = Math.min(1.0, Math.max(0.0, coeffs[0] - deltaPi0));
				coeffs[1] = Math.min(1.0-gradientStepSize, Math.max(gradientStepSize, coeffs[1] - deltaAlpha));
				coeffs[2] = Math.max(1.0+gradientStepSize, coeffs[2] - deltaBeta);

			}
			return avDelta;	
		}

		public double[][] shuffleEmpiricalCDF(double[][] pValues) {

			Random rndm = new Random();

			int ranSpot;
			double[] tmp;
			for (int i = 0; i < pValues.length; i++) {
				ranSpot = rndm.nextInt(pValues.length-i) + i;
				tmp = pValues[i].clone();
				pValues[i] = pValues[ranSpot].clone();
				pValues[ranSpot] = tmp.clone();
			}
			return pValues;
		}

		public double calcModelCDFValue(double p, double pi0, double alpha, double beta) {

			return pi0*p + (1-pi0)*new BetaDistribution(alpha, beta).cumulativeProbability(p);
		}

	}

	public static class FDRModelAveragingReducer extends Reducer<Text, Pi0AlphaBetaCountTuple, Text, Pi0AlphaBetaCountTuple> {

		private Pi0AlphaBetaCountTuple result = new Pi0AlphaBetaCountTuple();

		public void reduce(Text key, Iterable<Pi0AlphaBetaCountTuple> values, Context context) throws IOException, InterruptedException {

			double pi0 = 0;
			double alpha = 0;
			double beta = 0;
			long count = 0;
			for (Pi0AlphaBetaCountTuple val : values) {
				pi0 += val.getCount() * val.getPi0();
				alpha += val.getCount() * val.getAlpha();
				beta += val.getCount() * val.getBeta();
				count += val.getCount();
			}

			result.setCount(count);
			result.setPi0(pi0 / count);
			result.setAlpha(alpha / count);
			result.setBeta(beta / count);

			context.write(key, result);
		}
	}
}
