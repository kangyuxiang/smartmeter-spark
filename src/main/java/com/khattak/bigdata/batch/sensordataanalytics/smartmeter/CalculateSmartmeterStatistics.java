/*
 * Calculates 5 minute, hourly & daily power consumption for each city & 5 min forecasts
 * input path should be up to the day without the ending "/" after day 
 */

package com.khattak.bigdata.batch.sensordataanalytics.smartmeter;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.MinuteFrequency;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import com.cloudera.sparkts.models.HoltWinters;
import com.cloudera.sparkts.models.HoltWintersModel;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

public class CalculateSmartmeterStatistics {
	
	private static final Logger LOG = Logger.getLogger(CalculateSmartmeterStatistics.class);
	
	//how often to generate statistics (no. of seconds)
	private static final int REPEAT_TIME_PERIOD = 1 * 60; 
	
	public static void main(String[] args) throws IOException {
		
		if (args.length < 3) {
			System.err.println("Usage: $SPARK_HOME/bin/spark-submit --class com.khattak.bigdata.batch.sensordataanalytics.smartmeter.CalculateSmartmeterStatistics /opt/smartmeter-spark/smartmeter-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://localhost:8020/hadoop/data/smartmeter/raw_readings/2016/10/4 hdfs://localhost:8020/hadoop/data/smartmeter/statistics/city_usage hdfs://localhost:8020/hadoop/data/smartmeter/forecasts/city_usage/input/smartmeter_readings_5_minute.csv");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Smart meter statistics application");
		//JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SparkSession session = SparkSession
								.builder()
								.appName("Smart meter statistics application")
								.config(sparkConf)
								.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		
		//This does not report true number of files, reports quite more
		//Look for the sentence "Total input paths to process : xxx", which reports true number of files 
		//LOG.info("Total HDFS files to process: " + sc.textFile(args[0]).count());
		
		Path rootInputPath = null;
		Path rooOutputPath = null;
		
        try {
        	rootInputPath = new Path(args[0]);
        	rooOutputPath = new Path(args[1]);
        	
        	String rootOutputMinutePath = rooOutputPath.toString() + "/five_minute";
        	//String rootOutputHourPath = rooOutputPath.toString() + "/hourly";
        	//String rootOutputDayPath = rooOutputPath.toString() + "/daily";
       	
        	String outputMinuteFilePath = rootOutputMinutePath + "/" + rootInputPath.getParent().getParent().getName() + "/" + rootInputPath.getParent().getName() +"/" + rootInputPath.getName() + "/five_minute_statistics.csv";
        	//String forecastMinuteFilePath = rootOutputMinutePath + "/" + rootInputPath.getParent().getParent().getName() + "/" + rootInputPath.getParent().getName() +"/" + rootInputPath.getName() + "/five_minute_forecasts.csv";
        	String forecastMinuteFilePath = "/hadoop/data/smartmeter/forecasts/city_usage/output/five_minute_forecasts.csv";
        	
        	Calendar timestamp = Calendar.getInstance();// 5 min timestamp that will be written against the calculated values
        	//boolean hourlyConsumptionWritten = false;
        	
        	List<String> previousHoursMinutes = new ArrayList<String>();// list for keeping track of files that were processed on previous iteration of the below "while" loop
    		List<String> currentHoursMinutes = new ArrayList<String>();// list for keeping track of files that would be processed as part of the current iteration of the below "while" loop
    		
    		HoltWintersModel hwModel = null;
    		
        	while (true) {
        		
        		// Wait till XX:XX:15 time if the 5 min interval (raw readings) just completed
        		// Because that 5 min HDFS may still not be 100% complete if read straight away     		
        		Calendar rightNow = Calendar.getInstance();
        		if (rightNow.get(Calendar.MINUTE) % 5 == 0 && rightNow.get(Calendar.SECOND) < 15) {
        			int timeToSleep = 15 - rightNow.get(Calendar.SECOND);
        			LOG.info("main(): Sleeping for " + timeToSleep + " seconds!!!");
        			Thread.sleep(timeToSleep * 1000);
        		}
        		
        		//In HDFS, readings stored in year/month/day dir structure
        		//The input path goes down to the day level, so get hours & then minutes files
	        	List<Path> hours = GetDirectoryPaths(rootInputPath); 
		        if (hours != null && !hours.isEmpty()) {
		        	// list for storing all the files that need to be processed for all hours of the input day
		        	List<Triplet<String, Calendar, StatisticType>> filePathsWithTimestamps = new ArrayList<Triplet<String, Calendar, StatisticType>>();
		        	
		        	List<Pair<String, String>> hourData = new ArrayList<Pair<String, String>>();// master list that will contain the computed consumption for each city for each 5 min interval
		        	
		        	//need a sorted list as dirs are alphabetically sorted in HDFS
		        	Map<Integer, Path> sortedHours = new TreeMap<Integer, Path>();
		        	for (Path hourPath : hours){
		        		sortedHours.put(Integer.parseInt(hourPath.getName()), hourPath);
		        	}
		        	
		        	for (Map.Entry<Integer, Path> hourEntry : sortedHours.entrySet()){// loop for going through all hours of the input day 
		        		Path hourPath = hourEntry.getValue();
		        
		        		// Initialize the timestamp that will be set for each reading of the city to year/month/day/hour/0/0 for now
		        		// minute part will be set later as we get 5 min files for each hour
		        		timestamp.set(Integer.parseInt(hourPath.getParent().getParent().getParent().getName()), Integer.parseInt(hourPath.getParent().getParent().getName()) - 1, Integer.parseInt(hourPath.getParent().getName()), Integer.parseInt(hourPath.getName()), 0, 0);       		
		        		
		        		List<Path> minutes = GetFilePaths(hourPath);//get all 5 min files for each hour (5,10,,,,,60.csv)
		        		if (minutes != null && !minutes.isEmpty()) {
		        			Map<Integer, Path> sortedMinutes = new TreeMap<Integer, Path>();
		        			//need a sorted list as dirs are alphabetically sorted in HDFS
		        			for (Path minutePath : minutes){
		        				sortedMinutes.put(Integer.parseInt(minutePath.getName().split("\\.")[0]), minutePath);
				        	}
		        			
		        			for (Map.Entry<Integer, Path> minuteEntry : sortedMinutes.entrySet()){// loop for going through all minute files of the current hour that is in above loop
		        				
		        				Path minutePath = minuteEntry.getValue();
		    	        		//int hourTimestampPart = Integer.parseInt(hourPath.getName());
		    	        		int minuteTimestampPart = Integer.parseInt(minutePath.getName().split("\\.")[0]);//e.g. 60.csv
		    	        		
		    	        		//No need to do this as setting minutes equal to 60 automatically increments the hour & sets minutes to 0 
		    	        		////deal with last 5 min file of the hour by incrementing the hour
		    	        		/*if (minuteTimestampPart == 60) {// 60 means that this is the last 5 min reading file for that hour-- (55 -> 59 = 60.csv) 
		    	        			minuteTimestampPart = 0; 
		    	        			timestamp.add(Calendar.HOUR_OF_DAY, 1);
		    	        		}*/
		    	        		
		    	        		// 5,10,15,,,,,,55,0 (Although the last zero is for the next hour i.e. the hour would have been incremented by 1 automatically by Java when 60 is set as minutes)
		    	        		// This is not an issue as we are merely saying that the last 60.csv contains data between e.g. 22:55 -> 23:00
		    	        		// We are not allocating the last 60.csv readings to the next hour
		    	        		timestamp.set(Calendar.MINUTE, minuteTimestampPart);
		    	        		
		    	        		//exclude the current 5 minute csv file which is in the process of being updated
		    	        		//if we add this, then at the initial load of graph will show one value & when the graph updates after 5 mins, for the same timestamp,
		    	        		//a different value will be shown, hence only show fully updated 5 mins csv files
		    	        		Calendar now = Calendar.getInstance();
		    	        		now.setTimeZone(TimeZone.getTimeZone("GMT"));
		        				if (timestamp.after(now)) {
		        					LOG.info("main(): Skipping file >>> START <<<");
		        					LOG.info("main(): Skipped file >>> " + minutePath.toString());
		        					LOG.info("main(): Skipped file timestamp >>> " + timestamp.getTime().toString());
		        					LOG.info("main(): Now timestamp >>> " + now.getTime().toString());
		        					LOG.info("main(): Skipping file >>> END <<<");
		        					continue;
		        				}
		    	        		
		        				//LOG.info("main(): Now Timestamp >>> " + now.getTime().toString());
		        				//LOG.info("main(): File Timestamp >>> " + timestamp.getTime().toString());
		        				//LOG.info("main(): Path >>> " + minutePath.toString());
		        				
		        				filePathsWithTimestamps.add(new Triplet<String, Calendar, StatisticType>(minutePath.toString(), (Calendar)timestamp.clone(), StatisticType.FiveMinute));	    	        		
		    	        		currentHoursMinutes.add(hourEntry.getValue() + ":" + minuteEntry.getValue());
		    	        	}// End of loop for going through all minute files of the current hour that is in above loop
		    	        }
		        		else LOG.error("main(): GetFilePaths() method returned empty or null List!!!");
		        	}// End of loop for going through all hours of the input day 
		        	
		        	// Now that we have gone through all the hours for the input day, check if we actually need to process them as no new file might have been added since last run  
		        	boolean writeData = false;
		        	if (previousHoursMinutes.isEmpty()){// first run

		        		previousHoursMinutes.addAll(currentHoursMinutes);// copy from this run
		        		currentHoursMinutes.clear();// reset current list so that it's ready for next time when the for loop for hours is run
		        		writeData = true;
		        	}
		        	else if (!previousHoursMinutes.equals(currentHoursMinutes)){// current has one more than previous (in theory can be more than 1 but our while loop runs every minute so not really possible to have more than 1 file)
		        		
		        		previousHoursMinutes.clear();// reset previous list as it's stale
		        		previousHoursMinutes.addAll(currentHoursMinutes);// copy from this run
		        		currentHoursMinutes.clear();// reset current list so that it's ready for next time when the for loop for hours is run
		        		writeData = true;
		        	}
		        	else {
		        		currentHoursMinutes.clear();// reset current list so that it's ready for next time when the for loop for hours is run
		        		
		        		LOG.info("main(): currentHoursMinutes & previousHoursMinutes lists are same, so not rewriting HDFS file!!!");
		        	}
		        	
		        	if (writeData) {
		        		// 5 minute power consumption data for each day, will create 1 csv for each day & 1 entry for each 5 mins in that csv
		        		try {
		        			for (Triplet<String, Calendar, StatisticType> filePathsWithTimestamp : filePathsWithTimestamps) {
		        				
		        				// GenerateConsumptionStatistics() -> Pass in the context, full 5 minute file path & the type of statistic that needs calculating 
		        				// GenerateConsumptionStatistics() -> Returns a list of tuples (1 tuple per city) where each tuple consists of city and power consumption e.g. (London,0.0345)
		        				List<Tuple2<String, Double>> fiveMinuteStatistics = GenerateConsumptionStatistics(sc, filePathsWithTimestamp.getValue0(), filePathsWithTimestamp.getValue2()); 
		        				
		        				// Now convert to a csv friendly format with timestamp
		        				// ConvertToHeaderValuePair() -> The returned Pair consists of full header & a line that has timestamp & consumption figures for all cities for a specific timer interval e.g. 5 min
		    	        		Pair<String, String> minuteData = ConvertToHeaderValuePair(fiveMinuteStatistics, filePathsWithTimestamp.getValue1());
		    	        		hourData.add(minuteData);// keep adding all minute consumption figures, including timestamps across all 5 minute intervals for the input day
		        			}
		        			
		        			if (!hourData.isEmpty()) {
			        			// Write out 5 mins consumption data
		        				// GenerateConsumptionFileData() -> Creates CSV file ready to be written out
		        				// WriteToHDFS() -> Simply writes passed in string to the desired path
			        			boolean resultConsumptionFileWrite = WriteToHDFS(GenerateConsumptionFileData(hourData), outputMinuteFilePath);
			    				if (!resultConsumptionFileWrite) LOG.error("main(): HDFS file " + outputMinuteFilePath + " write ERROR!!!");
			    				
			    				// Fit & forecast via HoltWinters model by creating current input data *** Input data currently not used for fitting as not enough data, see second note below
			    				// GenerateForecastInputData() -> Returns the timeseries object that is then used for forecasting
			    				JavaTimeSeriesRDD<String> forecastInput = GenerateForecastInputData(session, sc, hourData);
			    				
			    				// Also, the number of min. readings should be 14 in forecastInput (the period of readings + 2 => (5 * 12 = 60) + 2)
			    				// The forecast method fails if forecastInput less then 12 & produces NANs forecasts if less than 14
			    				// So the solution is to either wait for an hour + 10 mins to get 14 readings or what we can do is to get at least 2 genuine current readings then interpolate rest of the 14
			    				// The model forecast method fails if readings less than 2 with message "Requires length of at least 2"
			    				// However, even if you provide 2 readings then it again fails with a "negative size array" message
			    				// So the trick is to at least wait for 2 readings (10 mins) then use the fill method to interpolate rest of the 14
			    				int forecastInputSize = forecastInput.first()._2().size();
			    				if (forecastInputSize > 1) {// Check if we have at least 2 readings
			    					LOG.info("main(): forecastInput size " + forecastInputSize);
			    					if (forecastInputSize < 14) {// the period (no. of readings per cycle, the hour)
			    						LOG.info("main(): Imputing " + (14 - forecastInputSize)  + " values using NEXT imputation method as number of input readings for forecasting less than 14!!!");
			    						forecastInput = ImputeData(forecastInput, (14 - forecastInputSize), 5, ImputationType.next); // complete 14 in total
			    					}
			    						
				    				// Currently, model is fit using old 5 min data for 1 full day as we don't have data for current full day 
			    					// HoltWinters is used as it is good with Seasonality & Trend
			    					// ARIMA wold have been a better choice based on model fitting in R
			    					// The auto.arima() function in R suggests a model with seasonal params, however, spark-ts library doesn't let you specify the seasonal component
			    					// See https://groups.google.com/forum/#!topic/spark-ts/yWUKpW64CHQ reply from Sandy
				    				if (hwModel == null) hwModel = FitHWModel(session, args[2]);// only create/train model once
				    				
				    				// Generate next 6  5-minute forecasts (half-hour)
				    				Map<String, List<Pair<ZonedDateTime, Double>>> forecasts = GenerateHoltWintersForecasts(hwModel, forecastInput, 6);
				    				
				    				if (!forecasts.isEmpty()) {
					    				// Write out forecast data
					    				boolean resultForecastFileWrite = WriteToHDFS(GenerateForecastFileData(forecasts, 6), forecastMinuteFilePath);
					    				if (!resultForecastFileWrite) LOG.error("main(): HDFS file " + forecastMinuteFilePath + " write ERROR!!!");
				    				}
			    				}
			    				else LOG.info("main(): Number of input readings for forecasting less than 2. Will try agin when the next 5 minute reading becomes available!!!");
		        			}
		    			} catch (IOException e) {
		    				e.printStackTrace();
		    			}
		        	}
		        }
		        else LOG.error("main(): GetDirectoryPaths() method returned empty or null List!!! Will try again after " + REPEAT_TIME_PERIOD + " seconds.");
		        
		        // Repeat every 1 minute
		        Thread.sleep(REPEAT_TIME_PERIOD * 1000);
        	}// End of main while loop
	     }
	     catch (Exception exp){
	    	 LOG.error("main(): Error " + exp.getMessage());
	     }
	    
	    //cityConsumption.saveAsTextFile(args[1]);
	    
        sc.close();
	}

	/*
	 * Returns a list of tuples where each tuple consists of city and power consumption
	 * The power consumption is calculated for the time period as specified by the statisticType param
	 * @param	sc  Current spark Context
	 * @param	inputPath The HDFS location of the csv file that contains the meter readings 
	 * @param	statisticType An enum value that specifies the type of consumption calculation required
 	 * @return	List of tuples each containing city & consumption (string, double)
	 */
	private static List<Tuple2<String, Double>> GenerateConsumptionStatistics(JavaSparkContext sc, String inputPath, StatisticType statisticType) throws IOException{
		
		//boolean result = false;
	
		JavaRDD<String> lines = sc.textFile(inputPath);
		
		JavaRDD<String> consumptionCommaSeparated = null;
		
		//Just for understanding
		/*consumptionCommaSeparated = lines.flatMap(
													new FlatMapFunction<String, String>() {
														private static final long serialVersionUID = 1L;
														
														@Override
														public Iterator<String> call(String x) {
															List<String> list = new ArrayList<String>();
															list.addAll(Arrays.asList(x.split(",")));
															return list.iterator();
													    }
													}
												);*/ 
						
					
		//using flatmap to get a single comma separated line as each row in the dataset. Using map provides an array, which is not easy to work with. 
		switch (statisticType) {
			case FiveMinute: {consumptionCommaSeparated = lines.flatMap( 
																		line -> {
																				String[] fields = line.split(","); 
																				String[] arrayOfLines = { fields[4] + ","  + fields[2], "UK,"  + fields[2]};
																				List<String> linesList = new ArrayList<String>();
																				linesList.addAll(Arrays.asList(arrayOfLines));
																				return linesList.iterator();
																			}
																	);}
				break;
		
			case Hourly:
			case Daily: 
			case Monthly: 
			case Yearly: {consumptionCommaSeparated = lines.map( line -> {String[] fields = line.split(","); return fields[0] + ","  + fields[1];});}//INVALID CODE...as the structure of file is different
			break;
			
			default:
				break;
		}

		//creating K,V pairs for each city including UK
		//each entry in the dataset will consist of city as K and consumption as V 
	    JavaPairRDD<String, Double> consumptionMap = consumptionCommaSeparated.mapToPair(line -> new Tuple2<String, Double>(line.split(",")[0], Double.parseDouble(line.split(",")[1])));

	    //now apply reduce so that we get one figure for each K by applying sum
	    // so in this case, all 15 second values for a particular city in the passed in 5 minute file would be aggregated together   
	    JavaPairRDD<String, Double> consumption = consumptionMap.reduceByKey( (s,k) -> { return s + k;} );

	    //Accumulate records from across all partitions of the RDD across all workers (if more than 1)
	    List<Tuple2<String, Double>> output = consumption.collect();

	    /*for (Tuple2<?,?> tuple : output) {
	      LOG.info("GenerateConsumptionStatistics(): " + tuple._1() + ": " + tuple._2());
	    }*/
		
		return output;
	}
	
    /*
     * Converts the list of tuples into a single Pair with just one header & values for each city.
     * Each passed in tuple has its own header & value (header being the key in the tuple)
     * @param	input List of tuples where each tuple has K,V based on city & consumption value
     * @param timestamp The timestamp to assign to this instance of readings for all cities
 	 * @return	A Pair that contains header (Timestamp,UK,Manchester,London,Birmingham,Glasgow) & the values (2016-10-29 10:00:00,0.1975,0.0447,0.0463,0.0535,0.0530)
     */
    private static Pair<String, String> ConvertToHeaderValuePair(List<Tuple2<String, Double>> input, Calendar timestamp){
    	
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	
    	String header = "Timestamp,";
    	String data = dateFormat.format(timestamp.getTime()) + ",";
    	
    	for (Tuple2<String, Double> datum : input) {
    		header += datum._1() + ",";
    		data += String.format("%.4f", datum._2()) + ",";
        }
    	
    	header = header.substring(0, header.length() -1 );
    	data = data.substring(0, data.length() -1 );
    	
    	//LOG.info("ConvertToHeaderValuePair(): Header >>> " + header + " ||||| data >>> " + data);
    	
    	return new Pair<String, String>(header, data);
    }
	
    /*
     * Writes out passed in string to the required HDFS path.
     * Deletes old file if already exists.
     * @param data String data that needs to be written to HDFS
     * @param path HDFS file that needs to be written. Deletes old file if already exists.
     * @return true if file written successfully 
     */
	private static boolean WriteToHDFS(String data, String path) throws IOException{
        Path outputPath = null;
        
        //Configuration conf = getConf();
        FSDataOutputStream outputStream = null;
        BufferedWriter br = null;
        FileSystem fs = GetHadoopFileSystemObject();
        //System.out.println("configured filesystem = " + conf.get("fs.defaultFS"));
        

	     try {
	         outputPath = new Path(path);
	         
	         if (fs.exists(outputPath)) {
	        	 fs.delete(outputPath, true);
	        	 LOG.info("WriteToHDFS(): Deleted file " + outputPath + " at " + new Date().toString());
	        	 //LOG.error("WriteToHDFS(): File " + outputPath + " ALEADY exists!!!");
	             //return false;
		     }
	 
	         outputStream = fs.create(outputPath);
	         
	         //IOUtils.copyBytes(is, os, getConf(), false);
	         
	         br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
	         br.write(data);
	         
	         LOG.info("WriteToHDFS(): Written to file: " + outputPath + " at " + new Date().toString());
	     }
	     catch (IOException exp){
	    	 LOG.error("WriteToHDFS(): Error " + exp.getMessage());
	     }
	     finally {
	    	 try {
	    		 if (br != null) br.close();
	    		 if (outputStream != null) outputStream.close();
			} catch (IOException e) {
				LOG.error("WriteToHDFS(): Error " + e.getMessage());
				e.printStackTrace();
				return false;
			}
	     }
        
        
        return true;
   }
    
	/*
	 * Returns a HDFS specific file system object that can be used to perform file manipulation operations
	 */
    private static FileSystem GetHadoopFileSystemObject() throws IOException{
    	Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        hadoopConf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
        return FileSystem.get(hadoopConf);
    }

    private static List<Path> GetDirectoryPaths(Path inputDirectoryPath){
    	List<Path> paths = null;
    	try {
    		boolean pathsFound = false;
    		FileSystem fs = GetHadoopFileSystemObject();
	    	if (fs.exists(inputDirectoryPath)) {
	       	 	RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listLocatedStatus(inputDirectoryPath);
	       	 	paths = new ArrayList<Path>();
	       	 	
	       	 	while(fileStatusListIterator.hasNext()) {
	       	 		pathsFound = true;
	       	 		LocatedFileStatus fileStatus = fileStatusListIterator.next();
	       	 		if (fileStatus.isDirectory()){
	       	 			paths.add(fileStatus.getPath());
	       	 		}
		       	 	else LOG.error("GetDirectoryPaths(): Path " + fileStatus.getPath() + "  is NOT a directory!!!");
	       	 	}
	       	 	if (!pathsFound) LOG.error("GetDirectoryPaths(): fs.listFiles for path " + inputDirectoryPath + " returned nothing!!!"); 
		    }
	        else LOG.error("GetDirectoryPaths(): Directrory path: " + inputDirectoryPath + "  DOESNOT exist!!!");
    	}
    	catch (IOException exp) {
    		LOG.error("GetDirectoryPaths(): Error " + exp.getMessage());
    	}
    	
    	return paths;
    }
    
    private static List<Path> GetFilePaths(Path inputDirectoryPath){
    	List<Path> paths = null;
    	try {
    		FileSystem fs = GetHadoopFileSystemObject();
	    	if (fs.exists(inputDirectoryPath)) {
	       	 	RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(inputDirectoryPath, false);
	       	 	paths = new ArrayList<Path>();
	       	 	//LOG.info("GetFilePaths(): Directrory path: " + inputDirectoryPath + " has following files...");
	       	 	while(fileStatusListIterator.hasNext()) {
	       	 		LocatedFileStatus fileStatus = fileStatusListIterator.next();
	       	 		if (fileStatus.isFile()){
	       	 			paths.add(fileStatus.getPath());
	       	 			//LOG.info("GetFilePaths(): " + fileStatus.getPath());
	       	 		}
		       	 	else LOG.error("GetFilePaths(): Path " + fileStatus.getPath() + " is NOT a File!!!");
	       	 	}
		    }
	        else LOG.error("GetFilePaths(): Directrory path: " + inputDirectoryPath + " DOESNOT exist!!!");
    	}
    	catch (IOException exp) {
    		LOG.error("GetFilePaths(): Error " + exp.getMessage());
    	}

    	return paths;
    }
    
    /*
     * Creates a single CSV file for all minute files across all hours of the day
     * @param hourData The list of Pairs containing 5 minute consumption figures across the entire day
     * @return File data that can be directly written to disk (HDFS)
     */
    private static String GenerateConsumptionFileData(List<Pair<String, String>> hourData){
    	    	
    	String header = "";
    	String values = "";
    	
    	for (Pair<String, String> line : hourData){
    		
    		if (header.isEmpty()) header = line.getValue0() + System.getProperty("line.separator");// the first element of the Pair contains the header which is same across all Pairs
    		
    		values += line.getValue1() + System.getProperty("line.separator");
    	}
    	
    	
    	return header + values;
    }
    
    /*
     * Converts Pairs of city,consumption to time series RDD
     * See https://blog.cloudera.com/blog/2015/12/spark-ts-a-new-library-for-analyzing-time-series-data-with-apache-spark/ for how a TS RDD is represented
     * @param session Current spark session. Required to create dataFrame Dataset<Row> inside the method
     * @param sc Current spark context
     * @param hourData List of Pairs where first value is the full header (including the timestamp of 5 min interval) & second value is the consumption figures for all cities including UK
     * @return A JavaTimeSeriesRDD<String> object
     */
	private static JavaTimeSeriesRDD<String> GenerateForecastInputData(SparkSession session, JavaSparkContext sc, List<Pair<String, String>> hourData) {
		// List of Triplets that becomes the basis of creating a dataframe later on
		// The Timestamp is the 5 min interval timestamp, String the city & Double is the consumption 
		List<Triplet<Timestamp,String,Double>> cityConsumptionList = new ArrayList<Triplet<Timestamp,String,Double>>();
		
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		// Header is same across all Pairs, so just get it from the first Pair in the List
		String[] splitHeader = hourData.get(0).getValue0().split(",");
		
		for (Pair<String, String> data : hourData){
			
			// splitValues contains consumption figures for each city including UK 
			String[] splitValues = data.getValue1().split(",");
    		for (int i = 1; i < splitHeader.length; i++){
    			try {
					cityConsumptionList.add(new Triplet<Timestamp,String,Double>(Timestamp.from(
																								dateFormatter.parse(splitValues[0]).toInstant()
																								),
																				splitHeader[i], 
																				Double.parseDouble(splitValues[i])));
    				
    				/*cityConsumptionList.add(new Triplet<ZonedDateTime,String,Double>(ZonedDateTime.ofInstant(
																								dateFormatter.parse(splitValues[0]).toInstant(),ZoneId.systemDefault()
																								),
																				splitHeader[i], 
																				Double.parseDouble(splitValues[i])));*/
    				
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
    		}
			
    	}

		// get first and last timestamps so that the bounds of the DateTimeIndex can then be built (later to be used for building JavaTimeSeriesRDD<String>)
		ZonedDateTime firstTimetsamp = ZonedDateTime.ofInstant(cityConsumptionList.get(0).getValue0().toInstant(), ZoneId.systemDefault());
		ZonedDateTime lastTimetsamp = ZonedDateTime.ofInstant(cityConsumptionList.get(cityConsumptionList.size() - 1).getValue0().toInstant(), ZoneId.systemDefault());
		
		////ZonedDateTime firstTS = cityConsumptionList.get(0).getValue0();
		////ZonedDateTime lastTS = cityConsumptionList.get(cityConsumptionList.size() - 1).getValue0();
		
		// Create RDD from the list
		JavaRDD<Triplet<Timestamp,String,Double>> cityConsumptionRDD = sc.parallelize(cityConsumptionList);
		
		// Apply map function to each record in the above RDD to get a Row
		// The end result is a Row based RDD that will be later used for creating a dataframe
		JavaRDD<Row> cityConsumptionRowRDD = cityConsumptionRDD.map(triplet -> {return RowFactory.create(triplet.getValue0(), triplet.getValue1(), triplet.getValue2());});
		
		// Declare schema for the dataframe
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
		fields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("consumption", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		
		//LOG.info("<<<BEFORE creating DF>>>");
		Dataset<Row> cityConsumptionDataFrame = session.createDataFrame(cityConsumptionRowRDD, schema);
		//LOG.info("FIRST row Timestamp: " + cityConsumptionDataFrame.first().getTimestamp(0).toString());
		//LOG.info("<<<AFTER creating DF>>>");
		
		//LOG.info("<<<BEFORE creating TS>>>");
		// Create a time based index with 5 minute intervals
		DateTimeIndex dtIndex = DateTimeIndexFactory.uniformFromInterval(firstTimetsamp, lastTimetsamp, new MinuteFrequency(5));
		// the last three params are the names of the timestamp, key & value columns from the schema (this is the makeup of the JavaTimeSeriesRDD)
		JavaTimeSeriesRDD<String> tsRDD = JavaTimeSeriesRDDFactory.timeSeriesRDDFromObservations(dtIndex, cityConsumptionDataFrame, "timestamp", "area", "consumption");
		
		/*LOG.info("<<<BEFORE filling TS>>>");
		Map<String, Vector> tempTSMap = tsRDD.collectAsMap();
		Set<String> tsKeys = tempTSMap.keySet();
		for (String key : tsKeys){
			Vector inputVector = tempTSMap.get(key);
			LOG.info("Input vector for " + key + ": " + inputVector.toString());
		}*/
		
		//Need to impute data for the timestamps for which there are no values else you get NaNs in model forecast
		// Filling methods include "linear", "nearest", "next", or "previous"
		JavaTimeSeriesRDD<String> filledTsRDD = tsRDD.fill("linear");
		
		//LOG.info("<<<AFTER filling TS>>>");
		/*tempTSMap = filledTsRDD.collectAsMap();
		tsKeys = tempTSMap.keySet();
		for (String key : tsKeys){
			Vector inputVector = tempTSMap.get(key);
			LOG.info("Input vector for " + key + ": " + inputVector.toString());
		}*/
		
		//LOG.info("<<<<<TS>>>>>");
		//LOG.info("TS Map size: " + tsRDD.collectAsMap().size());
		
		return filledTsRDD;
	}

	/*
	 * Fits a HoltWinters model based on the passed in data
	 */
	private static HoltWintersModel FitHWModel(SparkSession session, String inputFileForFittingModel){
		
		Dataset<String> lines = session.read().textFile(inputFileForFittingModel);
		Dataset<Double> doubleDataset = lines.map( line ->  Double.parseDouble(line), Encoders.DOUBLE());
		List<Double> doubleList = doubleDataset.collectAsList();
		
		Double[] doubleArray = new Double[doubleList.size()];
		doubleArray = doubleList.toArray(doubleArray); 
		
		double[] values = new double[doubleArray.length];
		for (int i = 0; i < doubleArray.length; i++){
			values[i] = doubleArray[i];
		}
		
		Vector tsVectorForFittingModel = Vectors.dense(values);
		HoltWintersModel hwModel = HoltWinters.fitModelWithBOBYQA(tsVectorForFittingModel, 12, "additive");//period is 12 as 5 mins readings every hour
		
		/*LOG.info("***HoltWinters Model Characteristics ***");
		LOG.info("Aplha:" + hwModel.alpha());
		LOG.info("Beta:" + hwModel.beta());
		LOG.info("Gamma:" + hwModel.gamma());
		LOG.info("Period:" + hwModel.period());
		LOG.info("Model type:" + hwModel.modelType());
		LOG.info("SSE:" + hwModel.sse(tsVectorForFittingModel));*/
		
			
		/*LOG.info("***HoltWinters Model Forecasts ***");
		Vector hwForecastVector = Vectors.dense(new double[]{0,0,0,0,0,0});
		Vector hwForecast = hwModel.forecast(tsVector, hwForecastVector);
		LOG.info("ARIMA model forecast for next 6 observations (half hour)>>>");
		
		for (int i = 0; i < hwForecast.size(); i++){
			LOG.info("Next " + ((i * 5) + 5) + " minutes: " +  hwForecast.toArray()[i]);
		}*/
		
		return hwModel;
	}
	
	/*
	 * Generates forecasts for the required number of observations
	 * @param hwModel The already fitted HW model
	 * @param currentReadingsTS The time series consisting of the data based on which the forecasts will be produced (today's data)
	 * @param noOfForecasts How many forecasts are required 
	 * return A map consisting of city as the key & a list of Pairs where each Pair consists of future timestamp & consumption forecast 
	 */
	private static Map<String, List<Pair<ZonedDateTime, Double>>> GenerateHoltWintersForecasts(HoltWintersModel hwModel, JavaTimeSeriesRDD<String> currentReadingsTS, int noOfForecasts){

		Map<String, List<Pair<ZonedDateTime, Double>>> forecasts = new HashMap<String, List<Pair<ZonedDateTime, Double>>>();
		
		Vector hwForecastVector = Vectors.dense(new double[noOfForecasts]);
		Map<String, Vector> inputTSMap = currentReadingsTS.collectAsMap();
		
		ZonedDateTime lastTimestamp = currentReadingsTS.index().last();
		
		// Get forecasts for each city
		Set<String> tsKeys = inputTSMap.keySet();
		for (String key : tsKeys){
			Vector inputVector = inputTSMap.get(key);
			//LOG.info("Input vector for " + key + ": " + inputVector.toString());
			Vector hwForecast = null;
			try {
				LOG.info("GenerateHoltWintersForecasts(): inputVector contents >>> " + inputVector.toString());
				/*LOG.info("GenerateHoltWintersForecasts(): hwForecastVector contents >>> " + hwForecastVector.toString());
				LOG.info("GenerateHoltWintersForecasts(): hwModel Alpha >>> " + hwModel.alpha());
				LOG.info("GenerateHoltWintersForecasts(): hwModel Beta >>> " + hwModel.beta());
				LOG.info("GenerateHoltWintersForecasts(): hwModel Gamma >>> " + hwModel.gamma());*/
				hwForecast = hwModel.forecast(inputVector, hwForecastVector);
			}
			catch (Exception exp){
				LOG.error("GenerateHoltWintersForecasts(): " + exp.getMessage());
				LOG.error("GenerateHoltWintersForecasts(): Exception details: " + exp.toString());
				LOG.error("GenerateHoltWintersForecasts(): Stack trace ...");
				exp.printStackTrace();
				LOG.error("GenerateHoltWintersForecasts(): Will try again when the next 5 minute statistics become available!!!");
				forecasts.clear();
				break;
			}
			//LOG.info("Next half-hour forecast for " + key + " >>>");
			
			List<Pair<ZonedDateTime, Double>> forecastsList = new ArrayList<Pair<ZonedDateTime, Double>>(); 
			Pair<ZonedDateTime, Double> forecast = null;
			for (int i = 0; i < hwForecast.size(); i++){
				//LOG.info("Next " + ((i * 5) + 5) + " minutes: " +  hwForecast.toArray()[i]);
				forecast = new Pair<ZonedDateTime, Double>(lastTimestamp.plusMinutes((i * 5) + 5),hwForecast.toArray()[i]);
				forecastsList.add(forecast);
			}
			forecasts.put(key,forecastsList);
			//forecasts.put(key, hwForecast.toArray());
		}
		
		return forecasts;
	}
    
	/*
	 * Converts the input map to a CSV file
	 */
    private static String GenerateForecastFileData(Map<String, List<Pair<ZonedDateTime, Double>>> forecasts, int noOfForecasts){
    	
    	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    	
    	String header = "Timestamp,";
    	String values = "";

    	//List<Pair<ZonedDateTime, Double>> forcastsList = null;
    	Set<String> forecastAreas = forecasts.keySet();
    	header += forecastAreas.toString().replace("[", "").replace("]", "").replace(" ", "") + System.getProperty("line.separator");
    	
    	boolean newLine = false;
    	for (int i = 0; i < noOfForecasts; i++){
    		for (int j = 0; j < forecastAreas.size(); j++){
    			
    			//header += "," + forecastAreas.toArray()[j];
    			
    			Pair<ZonedDateTime, Double> forecast = forecasts.get(forecastAreas.toArray()[j]).get(i);
    			if (!values.isEmpty() && !newLine) values += ",";
    			if (j == 0) values += forecast.getValue0().format(dateFormatter) + ",";
    			values += String.format("%.4f", forecast.getValue1());
    			
    			newLine = false;
    		}
    		values += System.getProperty("line.separator");
    		newLine = true;
    		/*for (Pair<ZonedDateTime, Double> forecast: forecasts.get(forecastAreas.toArray()[i])){
				values += forecast.getValue0().format(dateFormatter);
			}*/
    	}
    	
		/*for (String key : forecastAreas){
			header += "," + key;
			for (Pair<ZonedDateTime, Double> forecast: forecasts.get(key)){
				values += forecast.getValue0().format(dateFormatter);
			}
			 
		}*/   	
    	
    	return header + values;
    }

    /*
     * Method for filling in missing values in a avaTimeSeriesRDD<String> object
     */
    private static JavaTimeSeriesRDD<String> ImputeData(JavaTimeSeriesRDD<String> tsTobeImputed, int noOfImputations, int minuteInterval, ImputationType imputationType){
    	
    	// go back in time
    	ZonedDateTime firstTimestamp = tsTobeImputed.index().first().minusMinutes(noOfImputations * minuteInterval);
    	DateTimeIndex newDtIndex = DateTimeIndexFactory.uniformFromInterval(firstTimestamp, tsTobeImputed.index().last(), new MinuteFrequency(5));
    	JavaTimeSeriesRDD<String> imputedTS = tsTobeImputed.withIndex(newDtIndex);
    	return imputedTS.fill(imputationType.toString());
    }
    
    private enum StatisticType {
    	FiveMinute, Hourly, Daily, Monthly, Yearly
    } 
    
    private enum ImputationType {
    	linear, nearest, next, previous, spline, zero
    } 
}
