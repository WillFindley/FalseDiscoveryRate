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

public class MapReduceCDFFalseDiscoveryRate extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("\n" + 
					"This program runs a mapreduce to determine the coefficients for a beta-uniform model of the p-value CDF \n" +  
					"Usage is: \n\n" +
					"hadoop jar [jarFile] MapReduceCDFFalseDiscoveryRate [args0] [args1] [args2] \n\n" + 
					"args0 - input path of p-values \n" +
					"args1 - output path of coefficients \n" +
					"args2 - number of p-values for each map's independent BUM fit \n"
					);
			return;
		}
		int res = ToolRunner.run(new Configuration(), new MapReduceCDFFalseDiscoveryRate(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		
		conf.set("numSamplesForFit", args[2]);

		Job job = Job.getInstance(conf, "MapReduceCDFFalseDiscoveryRate");
		job.setJarByClass(MapReduceCDFFalseDiscoveryRate.class);

		job.setJobName("calcBUM");
		
		job.setMapperClass(FDRCalculationMapping.class);
		job.setCombinerClass(FDRModelAveragingReducer.class);
		job.setReducerClass(FDRModelAveragingReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pi0AlphaBetaCountTuple.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Pi0AlphaBetaCountTuple.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class FDRCalculationMapping extends Mapper<Object, Text, Text, Pi0AlphaBetaCountTuple> {

		// allContribute is only one text entry because everything will be averaged together in the reducer 
		private Text allContribute = new Text("BUM coefficients");
		// stores the coefficients for the beta-uniform mixture in a tuple
		private Pi0AlphaBetaCountTuple coeffAns = new Pi0AlphaBetaCountTuple();
		// adds on p-value a p-value with each mapped xml line read until complete for BUM fit 
		private ArrayList<Double> tmpPValues = new ArrayList<Double>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			tmpPValues.add(transformXmlToPValues(value.toString()));

			// the number of p-values to collect before doing the BUM fitting
			int numSamplesForFit = Integer.parseInt(context.getConfiguration().get("numSamplesForFit"));
			if (tmpPValues.size() == numSamplesForFit) {

				double[][] pValues = calculateEmpiricalCDF(tmpPValues.toArray(new Double[numSamplesForFit]));
				// now that the p-values have been used to compute their empirical CDF, re-initialize it for the next round of map reads
				tmpPValues = new ArrayList<Double>();

				// calculate the optimal coefficients using stochastic gradient descent
				double[] coeffs = getOptCoeffs(pValues);

				// fill out the BUM coefficients tuple
				coeffAns.setPi0(coeffs[0]);
				coeffAns.setAlpha(coeffs[1]);
				coeffAns.setBeta(coeffs[2]);
				coeffAns.setCount(1);

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

			// parses the xml line into the single p-value that is returned
			return Double.parseDouble(xml.substring(startIndex,stopIndex));
		}

		private double[][] calculateEmpiricalCDF(Double[] tmpPValues) {

			// set up the empirical CDF for the p-values
			double[][] pValues = new double[tmpPValues.length][2];
			// the first column will have a p-value in each row
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
			// start off with high learning steps and rates
			double avDelta = 1;
			double oldDelta = avDelta;
			double learningRate = 2;

			int sigDigits = 4; // number of significant digits in the model parameters;
			double tolerance = 1.0 / Math.pow(10,sigDigits); // the decimal place to which the significance corresponds
			do {
				oldDelta = avDelta;
				// coeffs is implicitly returned because it is modified at the reference position
				avDelta = stochasticGradientDescent(pValues, coeffs, tolerance, learningRate);
				if (oldDelta < avDelta) learningRate *= 0.9;  // anneal the learning rate every time a local minimum is passed
			} while (avDelta >= tolerance);  // once the tolerance is met, exit to return the coefficients

			return coeffs;	
		}

		private double stochasticGradientDescent(double[][] pValues, double[] coeffs, double gradientStepSize, double learningRate) {

			// shuffling is necessary for the stochastic presentation of p-values for the gradient descent (no batching)
			pValues = shuffleEmpiricalCDF(pValues);

			// initial overall average parameter movement
			double avDelta = 0;
			// this momentum is always used (average of previous and current delta change for next step)
			double momentum = 0.5;
			// initial amounts of parameter movement
			double deltaPi0 = 0;
			double deltaAlpha = 0;
			double deltaBeta = 0;
			// takes a gradient descent step for each p-value
			for (double[] data : pValues) {

				// moves along the coefficient gradient for a single p-value measurement
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

				// updates the user on what the current average error is
				System.out.println("Current average error: " + avDelta);

				// rectifies to coeffcients to ensure that pi0 and alpha are always between 0 and 1 and beta is always greater than 1 
				coeffs[0] = Math.min(1.0, Math.max(0.0, coeffs[0] - deltaPi0));
				coeffs[1] = Math.min(1.0-gradientStepSize, Math.max(gradientStepSize, coeffs[1] - deltaAlpha));
				coeffs[2] = Math.max(1.0+gradientStepSize, coeffs[2] - deltaBeta);

			}
			return avDelta;	
		}

		public double[][] shuffleEmpiricalCDF(double[][] pValues) {

			Random rndm = new Random();

			// this is just a Fisher-Yates shuffle of the empirical CDF rows
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

			// what should the CDF value be for the p-value if the current coefficients are correct
			return pi0*p + (1-pi0)*new BetaDistribution(alpha, beta).cumulativeProbability(p);
		}

	}

	public static class FDRModelAveragingReducer extends Reducer<Text, Pi0AlphaBetaCountTuple, Text, Pi0AlphaBetaCountTuple> {

		// reduce all of the mapped tuple results down to this one result
		private Pi0AlphaBetaCountTuple result = new Pi0AlphaBetaCountTuple();

		public void reduce(Text key, Iterable<Pi0AlphaBetaCountTuple> values, Context context) throws IOException, InterruptedException {

			double pi0 = 0;
			double alpha = 0;
			double beta = 0;
			long count = 0;
			// aggregate all fo the mapped tuples for calculating the final BUM coefficients
			for (Pi0AlphaBetaCountTuple val : values) {
				pi0 += val.getCount() * val.getPi0();
				alpha += val.getCount() * val.getAlpha();
				beta += val.getCount() * val.getBeta();
				count += val.getCount();
			}

			// do the averages to get the final BUM coefficients answer for outputting
			result.setCount(count);
			result.setPi0(pi0 / count);
			result.setAlpha(alpha / count);
			result.setBeta(beta / count);

			context.write(key, result);
		}
	}
}
