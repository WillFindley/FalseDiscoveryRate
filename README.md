# A BUM CDF MapReduce FDR program

CDF-based Beta-Uniform Mixture model for determining False Discovery Rate-based significant findings in Hadoop

## Installation

download the jar file

install [Apache Commons Math](http://commons.apache.org/proper/commons-math/)

## Usage

1. (optional) run RandomDataGenerationDriver class

  This program generates a mixed uniform beta distribution of p-values randomly distributed throughout HDFS for testing False Discovery Rate protocols.  
  Usage is: 

  hadoop jar MRCDFFDR.jar RandomDataGenerationDriver [args0] [args1] [args2] [args3] [args4] [args5] 

  args0 - number of mapper tasks  
  args1 - number of records produced by each mapper  
  args2 - pi0, the proportion of p-values that are uniformly distributed (false hypotheses)  
  args3 - alpha, the alpha for the beta distribution; less than one yields smaller values (strong true hypotheses)  
  args4 - beta, the beta for the beta distribution; greater than one yields larger values (weak true hypotheses)  
  args5 - slave directory in which to write p-value xml.

2. run MapReduceCDFFalseDiscoveryRate class

  This program runs a mapreduce to determine the coefficients for a beta-uniform model of the p-value CDF  
  Usage is: 

  hadoop jar MRCDFFDR.jar MapReduceCDFFalseDiscoveryRate [args0] [args1] [args2] 

  args0 - input path of p-values  
  args1 - output path of coefficients  
  args2 - number of p-values for each map's independent BUM fit  

3. run MapReduceSignificantFindings class

  This program runs a mapreduce to determine the p-value entries that are significant at the FDR cutoff  
  Usage is: 

  hadoop jar MRCDFFDR.jar MapReduceSignificantFindings [args0] [args1] [args2] [args3] [args4] [args5] 

  args0 - input path of p-values  
  args1 - output path to significant findings  
  args2 - pi0 proportion of null hypotheses  
  args3 - alpha for the beta distribution for the true hypotheses  
  args4 - beta for the beta distribution for the true hypotheses  
  args5 - false discovery rate cutoff for significance  


## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

Alpha version 0.0001 on 06/26/2015

## Credits

Will Findley
