package com.cloudera.tsexamples;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Comparator;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class JavaStocks {
	
	private static final Logger LOG = Logger.getLogger(JavaStocks.class);
	
	public static void main(String[] args) {
		if (args.length != 1){
	      System.err.println("Usage: JavaStocks <hdfs path to ARIMA.txt>");
		  System.exit(1);
		}
	  
		SparkConf conf = new SparkConf()
									.setAppName("Spark-TS Ticker Example")
									.setMaster("local");
		conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
		
		SparkSession session = SparkSession
								.builder()
								.appName("Spark-TS Ticker Example")
								.config(conf)
								.getOrCreate();

		Dataset<String> lines = session.read().textFile(args[0]);
		
		Dataset<Double> doubleDataset = lines.map( line ->  Double.parseDouble(line), Encoders.DOUBLE());
		
		List<Double> doubleList = doubleDataset.collectAsList();
		
		Double[] doubleArray = new Double[doubleList.size()];
		doubleArray = doubleList.toArray(doubleArray); 
		
		double[] values = new double[doubleArray.length];
		for (int i = 0; i < doubleArray.length; i++){
			values[i] = doubleArray[i];
		}
		
		Vector ts = Vectors.dense(values);
		LOG.info("TS Vector:" + ts.toString());
		
		ARIMAModel arimaModel = ARIMA.autoFit(ts, 1, 0, 1);
		
		LOG.info("***ARIMA Coefficients ***");
		for (double coefficient : arimaModel.coefficients()){
			LOG.info("ARIMA Coefficient:" + coefficient);
		}
		
		Vector forecast = arimaModel.forecast(ts, 10);
		LOG.info("ARIMA Forecast for next 10 observations:" + forecast);
			
		/*Dataset<Row> tickerObs = loadObservations(session, args[0]);
			
		
		// Create a daily DateTimeIndex over August and September 2015
		ZoneId zone = ZoneId.systemDefault();
		DateTimeIndex dtIndex = DateTimeIndexFactory.uniformFromInterval(
																		ZonedDateTime.of(LocalDateTime.parse("2015-08-03T00:00:00"), zone),
																		ZonedDateTime.of(LocalDateTime.parse("2015-09-22T00:00:00"), zone),
																		new BusinessDayFrequency(1, 0)
																		);
			
		// Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
		JavaTimeSeriesRDD<String> tickerTsRdd = JavaTimeSeriesRDDFactory.timeSeriesRDDFromObservations(dtIndex, tickerObs, "timestamp", "symbol", "price");
		

		// Cache it in memory
		tickerTsRdd.cache();
		
		// Count the number of series (number of symbols)
		LOG.info("Number of symbols in ticker file:" + tickerTsRdd.count());
		
		// Impute missing values using linear interpolation
		JavaTimeSeriesRDD<String> filled = tickerTsRdd.fill("linear");
		
		// Compute return rates
		JavaTimeSeriesRDD<String> returnRates = filled.returnRates();
		
		// Compute Durbin-Watson stats for each series
		JavaPairRDD<String, Double> dwStats = returnRates.mapValues(
																	(Vector x) -> TimeSeriesStatisticalTests.dwtest(x)
																	);
		
		
		LOG.info("Durbin-Watson min:" + dwStats.min(new StatsComparator()));
		LOG.info("Durbin-Watson max:" + dwStats.max(new StatsComparator()));
		
		JavaRDD<ARIMAModel> models = filled.map(
												(Tuple2<String, Vector> ts) -> { return ARIMA.autoFit(ts._2, 1, 0, 1); }
											);
			
		ARIMAModel model = models.first();
		
		LOG.info("ARIMA Coefficients:" + model.coefficients());
		
		Vector forecast = model.forecast(filled.first()._2, 10);
		
		LOG.info("ARIMA Forecast for next 10 observations:" + forecast);*/
		
		session.stop();
    }
  
	private static Dataset<Row> loadObservations(SparkSession session, String path) {

		//Encoder<String> stringEncoder = Encoders.STRING();
		//Encoder<Stock> stockEncoder = Encoders.bean(Stock.class);
		
		JavaRDD<String> stocks = session.sparkContext().textFile(path,1).toJavaRDD();//.as(stockEncoder);
		
		JavaRDD<Row> stockRDD = stocks.map((String line) -> {
		
		//LOG.info("Ticker data: " + line);
		
		String[] tokens = line.split("\t");
				ZonedDateTime dt = ZonedDateTime.of(Integer.parseInt(tokens[0]),
	      											Integer.parseInt(tokens[1]), Integer.parseInt(tokens[1]), 0, 0, 0, 0,
	      											ZoneId.systemDefault()
	  												);
				String symbol = tokens[3];
				double price = Double.parseDouble(tokens[5]);
				return RowFactory.create(Timestamp.from(dt.toInstant()), symbol, price);
		});
		
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
		fields.add(DataTypes.createStructField("symbol", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> stockDataFrame = session.createDataFrame(stockRDD, schema);
		
		//stockDataFrame.printSchema();
		
		//stockDataFrame.show();
		
		/*/*Dataset<Row> stockDataset = data.map((String line) -> {
		
			LOG.info("Ticker data: " + line);
			
			String[] tokens = line.split("\t");
					ZonedDateTime dt = ZonedDateTime.of(Integer.parseInt(tokens[0]),
		      											Integer.parseInt(tokens[1]), Integer.parseInt(tokens[1]), 0, 0, 0, 0,
		      											ZoneId.systemDefault()
		  												);
					String symbol = tokens[3];
					double price = Double.parseDouble(tokens[5]);
					return RowFactory.create(Timestamp.from(dt.toInstant()), symbol, price);
			}, stockEncoder);
		  
		JavaRDD<Stock> javaRDD = stockDataset.toJavaRDD();
	  
		List<StructField> fields = new ArrayList();
		fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
		fields.add(DataTypes.createStructField("symbol", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		//return sqlContext.createDataFrame(rowRdd, schema);
		return session.createDataFrame(javaRDD, schema);**/
  
		return stockDataFrame;
    }

	private static class Stock implements Serializable {
	    private int year;
	    private int month;
	    private int day;
	    private String symbol;
	    private long volume;
	    private double price;

	    public String getSymbol() {
	      return symbol;
	    }
	    public void setSymbol(String symbol) {
	      this.symbol = symbol;
	    }

	    public int getYear() {
	      return year;
	    }
	    public void setYear(int year) {
	      this.year = year;
	    }
	    
	    public int getMonth() {
	    	return month;
		}
	    public void setMonth(int month) {
	      this.month = month;
	    }
	    
	    public int getDay() {
		  return day;
		}
	    public void setDay(int day) {
	      this.day = day;
	    }
	    
	    public long getVolume() {
		  return volume;
		}
	    public void setVolume(long volume) {
	      this.volume = volume;
	    }
	    
	    public double getPrice() {
		  return price;
		}
	    public void setPrice(double price) {
	      this.price = price;
	    }
	}

	private static class StatsComparator implements Comparator<Tuple2<String,Double>>, java.io.Serializable {
		private static final long serialVersionUID = 1L;

		public int compare(Tuple2<String, Double> a, Tuple2<String, Double> b) {
	        return a._2.compareTo(b._2);
	    }
	}
}