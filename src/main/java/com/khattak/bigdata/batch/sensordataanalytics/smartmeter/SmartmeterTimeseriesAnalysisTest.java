package com.khattak.bigdata.batch.sensordataanalytics.smartmeter;

import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;
import com.cloudera.sparkts.models.HoltWinters;
import com.cloudera.sparkts.models.HoltWintersModel;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SmartmeterTimeseriesAnalysisTest {
	
	private static final Logger LOG = Logger.getLogger(SmartmeterTimeseriesAnalysisTest.class);

	public static void main(String[] args) {
		if (args.length != 1){
		      System.err.println("Usage: JavaStocks <hdfs path to smartmeter_readings_5_minute.csv>");
			  System.exit(1);
		}
	  
		SparkConf conf = new SparkConf()
									.setAppName("Smartmeter 5 Minute Forecasts")
									.setMaster("local");
		conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
		
		SparkSession session = SparkSession
								.builder()
								.appName("Smartmeter 5 Minute Forecasts")
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
		
		Vector tsVector = Vectors.dense(values);
		LOG.info("5 minute smartmeter readings vector:" + tsVector.toString());
		
		ARIMAModel arimaModel = ARIMA.autoFit(tsVector, 1, 0, 1);
		
		LOG.info("***ARIMA Model Coefficients ***");
		for (double coefficient : arimaModel.coefficients()){
			LOG.info("ARIMA Coefficient:" + coefficient);
		}
		
		LOG.info("***ARIMA Model Forecasts ***");
		Vector arimaForecastVector = arimaModel.forecast(tsVector, 6);
		LOG.info("ARIMA model forecast for next 6 observations (half hour)>>>");
		int total = arimaForecastVector.size();
		
		int interval = 5;
		int intervalCounter = 0;
		for (int i = total - 6; i < total; i++){
			LOG.info("Next " + ((intervalCounter * interval) + interval) + " minutes: " +  arimaForecastVector.toArray()[i]);
			intervalCounter += 1;
		}
		
		
		HoltWintersModel hwModel = HoltWinters.fitModelWithBOBYQA(tsVector, 12, "additive");
		
		LOG.info("***HoltWinters Model Characteristics ***");
		LOG.info("Aplha:" + hwModel.alpha());
		LOG.info("Beta:" + hwModel.beta());
		LOG.info("Gamma:" + hwModel.gamma());
		LOG.info("Period:" + hwModel.period());
		LOG.info("Model type:" + hwModel.modelType());
		LOG.info("SSE:" + hwModel.sse(tsVector));
		
			
		LOG.info("***HoltWinters Model Forecasts ***");
		Vector hwForecastVector = Vectors.dense(new double[]{0,0,0,0,0,0});
		Vector hwForecast = hwModel.forecast(tsVector, hwForecastVector);
		LOG.info("HoltWinters model forecast for next 6 observations (half hour)>>>");
		
		for (int i = 0; i < hwForecast.size(); i++){
			LOG.info("Next " + ((i * 5) + 5) + " minutes: " +  hwForecast.toArray()[i]);
		}
		//LOG.info(forecastVector.toString());// gives same results as above
		
		session.stop();
	}

}
