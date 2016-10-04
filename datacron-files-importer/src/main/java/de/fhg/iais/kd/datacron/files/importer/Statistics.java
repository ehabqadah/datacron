package de.fhg.iais.kd.datacron.files.importer;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.files.importer.table.beans.TBOutput;
import de.fhg.iais.kd.datacron.files.importer.util.Utils;

public class Statistics implements Serializable {

	private static final long serialVersionUID = 1072219813274243606L;

	@Inject
	@Named("spark.app.trajectories.statistics")
	private String statisticsOutputDir;

	@Inject
	private Mapping mapping;

	@Inject
	private transient Utils utils;

	/**
	 * Generate input data statistics:
	 * 
	 * N-Points
	 * 
	 * Min/Max time
	 * 
	 * Min/Max coordinates (bounding rectangle)
	 * 
	 * @param trajectoriesRDD
	 */
	public void generate(JavaRDD<TBOutput> dataRDD, final String name) {

		// Create pair1RDD:(TableId, InputDataBean)
		JavaPairRDD<String, TBOutput> pair1RDD = dataRDD.keyBy(input -> name);

		// Cache dataRDD
		pair1RDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

		// 0. Compute n points for for whole input data set
		this.generatePointStatistics(pair1RDD);

		// 1. Compute time statistics for whole input data set:
		this.generateTimeStatistics(pair1RDD);

		// 2. Compute coordinates statistics for whole input data set:
		this.generateCoordinatesStatistics(pair1RDD);

	}

	/**
	 * 
	 * Nr of points of whole dataset
	 * 
	 * @param inputRDD:
	 *            input data RDD
	 */
	private void generatePointStatistics(JavaPairRDD<String, TBOutput> inputRDD) {
		Map<String, Object> points = inputRDD.countByKey();

		// Save point statistics to file:
		 utils.appendToFile("wholeData.txt", "NPoints: " + points.entrySet().iterator().next().getValue());
	}

	/**
	 * Compute time statistics
	 * 
	 * @param inputRDD
	 */
	private void generateTimeStatistics(JavaPairRDD<String, TBOutput> inputRDD) {
		// Create dateRDD: (TrajectoryId, Date)
		String minDateString;
		String maxDateString;

		if (mapping.isPhysicaltime()) {
			JavaPairRDD<String, DateTime> date1RDD = inputRDD.mapValues(input -> {
				return DateTime.parse(input.getD());
			});

			// Cache dateRDD
			date1RDD.cache();

			// Compute minDateRDD pro trajectory: (TrajectoryId, MinDate)
			JavaPairRDD<String, DateTime> minDateRDD = this.computeMinDateTime(date1RDD);
			minDateString = minDateRDD.first()._2().toString();

			// Compute maxDateRDD pro trajectory: (TrajectoryId, MaxDate)
			JavaPairRDD<String, DateTime> maxDateRDD = this.computeMaxDateTime(date1RDD);
			maxDateString = maxDateRDD.first()._2().toString();
		} else {
			JavaPairRDD<String, Long> date2RDD = inputRDD.mapValues(input -> {
				return Long.valueOf(input.getD());
			});
			// Cache dateRDD
			date2RDD.cache();

			// Compute minDateRDD pro trajectory: (TrajectoryId, MinDate)
			JavaPairRDD<String, Long> minDateRDD = this.computeMin(date2RDD);
			minDateString = minDateRDD.first()._2().toString();

			// Compute maxDateRDD pro trajectory: (TrajectoryId, MaxDate)
			JavaPairRDD<String, Long> maxDateRDD = this.computeMax(date2RDD);
			maxDateString = maxDateRDD.first()._2().toString();
		}

		// Save time statistics to file:
		 utils.appendToFile("wholeData.txt", "Min date: " + minDateString, "Max date: " + maxDateString);
	}

	/**
	 * Compute coordinates statistics of whole data set
	 * 
	 * (bounding rectangle)
	 * 
	 * @param inputRDD:
	 *            input data RDD
	 */
	private void generateCoordinatesStatistics(JavaPairRDD<String, TBOutput> inputRDD) {
		// Compute xRDD = (TrajectoryId, XCoordinate)
		JavaPairRDD<String, Double> xRDD = inputRDD.mapValues(trajectory -> {
			return Double.valueOf(trajectory.getX());
		});

		// Cache xRDD
		xRDD.cache();

		// Compute minXRDD of whole input data set: (TableId, MinX)
		JavaPairRDD<String, Double> minXRDD = this.computeMin(xRDD);

		// Compute maxXRDD of whole input data set: (TableId, MaxX)
		JavaPairRDD<String, Double> maxXRDD = this.computeMax(xRDD);

		// Compute yRDD = (TableId, yCoordinate)
		JavaPairRDD<String, Double> yRDD = inputRDD.mapValues(trajectory -> {
			return Double.valueOf(trajectory.getY());
		});

		// Cache yRDD
		yRDD.cache();

		// Compute minyRDD of whole input data set: (TableId, MinY)
		JavaPairRDD<String, Double> minYRDD = this.computeMin(yRDD);

		// Compute maxyRDD of whole input data set: (TableId, MaxY)
		JavaPairRDD<String, Double> maxYRDD = this.computeMax(yRDD);

		String minX = minXRDD.first()._2().toString();
		String maxX = maxXRDD.first()._2().toString();
		String minY = minYRDD.first()._2().toString();
		String maxY = maxYRDD.first()._2().toString();

		// Save coordinates statistics to file:
		 utils.appendToFile("wholeData.txt", "Min X: " + minX, "Max X: " + maxX, "Min Y: " + minY, "Max Y: " + maxY);
	}

	/**
	 * Compute min of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Min date or Min coordinate)
	 */
	private <T extends Comparable<T>> JavaPairRDD<String, T> computeMin(JavaPairRDD<String, T> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) < 0 ? v1 : v2);
		});
	}

	/**
	 * Compute max of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Max date or Max coordinate)
	 */
	private <T extends Comparable<T>> JavaPairRDD<String, T> computeMax(JavaPairRDD<String, T> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) > 0 ? v1 : v2);
		});
	}

	/**
	 * Compute min of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Min date or Min coordinate)
	 */
	private JavaPairRDD<String, DateTime> computeMinDateTime(JavaPairRDD<String, DateTime> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) < 0 ? v1 : v2);
		});
	}

	/**
	 * Compute max of dates or coordinates of whole data
	 * 
	 * @param pairRDD:
	 *            (Table, dates or coordinates)
	 * @return pairRDD: (Table, Max date or Max coordinate)
	 */
	private JavaPairRDD<String, DateTime> computeMaxDateTime(JavaPairRDD<String, DateTime> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) > 0 ? v1 : v2);
		});
	}
}
