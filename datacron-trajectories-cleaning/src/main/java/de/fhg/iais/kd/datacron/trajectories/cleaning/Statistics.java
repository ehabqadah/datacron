package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBTrajectoriesOutput;
import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
public class Statistics implements Serializable {

	private static final long serialVersionUID = 5418827747835053055L;

	@Inject
	@Named("spark.app.trajectories.statistics")
	private String statisticsOutputDir;

	@Inject
	@Named("spark.app.trajectories.speedoutlierthresholdfactor")
	private double speedThresholdFactor;

	/**
	 * Generate trajectories statistics:
	 * 
	 * N-Points
	 * 
	 * Min/Max/Avg/Median of speed, acceleration and diff time
	 * 
	 * Min/Max duration
	 * 
	 * Min/Max coordinates (bounding rectangle)
	 * 
	 * @param trajectoriesRDD
	 */
	public void generate(JavaRDD<TBTrajectoriesOutput> trajectoriesRDD) {
		// Create pair1RDD:(TrajectoryId, TrajectoryBean)
		JavaPairRDD<String, TBTrajectoriesOutput> pair1RDD = trajectoriesRDD.keyBy(trajectory -> trajectory.getId());

		// Cache pair1RDD
		pair1RDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

		// 0. Compute n points for each trajectory
		this.generatePointStatistics(pair1RDD);

		// 1. Compute speed statistics for each trajectory:
		this.generateSpeedStatistics(pair1RDD);

		// 2. Compute acceleration statistics for each trajectory:
		this.generateAccelerationStatistics(pair1RDD);

		// 3. Compute diff time statistics for each trajectory:
		this.generateDiffTimeStatistics(pair1RDD);

		// 4. Compute duration statistics for each trajectory:
		this.generateDurationStatistics(pair1RDD);

		// 5. Compute coordinates statistics for each trajectory:
		this.generateCoordinatesStatistics(pair1RDD);

	}

	/**
	 * 
	 * Nr of points for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 */
	private void generatePointStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		Map<String, Object> points = inputRDD.countByKey();

		this.writeToFile(points);
	}

	/**
	 * Compute speed statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 */
	private void generateSpeedStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		// Create speedRDD: (TrajectoryId, TrajectorySpeed)
		JavaPairRDD<String, Double> speedRDD = calculateSpeedRDD(inputRDD);

		// Cache speedRDD
		speedRDD.cache();

		// Compute minSpeedRDD: (TrajectoryId, MinSpeed)
		JavaPairRDD<String, Double> minSpeedRDD = this.computeMinDouble(speedRDD);
		minSpeedRDD.saveAsTextFile(statisticsOutputDir + "/minSpeedPerTrajectory");

		// Compute maxSpeedRDD: (TrajectoryId, MaxSpeed)
		JavaPairRDD<String, Double> maxSpeedRDD = this.computeMaxDouble(speedRDD);
		maxSpeedRDD.saveAsTextFile(statisticsOutputDir + "/maxSpeedPerTrajectory");

		// Compute avgSpeedRDD: (TrajectoryId, AvgSpeed)
		JavaPairRDD<String, Double> avgSpeedRDD = this.computeAverage(speedRDD);
		avgSpeedRDD.saveAsTextFile(statisticsOutputDir + "/avgSpeedPerTrajectory");

		// Compute medianSpeedRDD: (TrajectoryId, MedianSpeed)
		JavaPairRDD<String, Double> medianSpeedRDD = this.computeMedian(speedRDD);
		medianSpeedRDD.cache();
		medianSpeedRDD.saveAsTextFile(statisticsOutputDir + "/medianSpeedPerTrajectory");
	}

	public JavaPairRDD<String, Double> detectSpeedOutliers(JavaPairRDD<String, Double> medianSpeedRDD) {
		double medianMedianSpeed = this.computeMedian(medianSpeedRDD.mapToPair(pair -> new Tuple2<>("ALL", pair._2())))
				.first()._2();
		final double speedThreshold = speedThresholdFactor * medianMedianSpeed;
		JavaPairRDD<String, Double> speedOutliersRDD = medianSpeedRDD.filter(tuple -> tuple._2() > speedThreshold);
		return speedOutliersRDD;
	}

	public JavaPairRDD<String, Double> calculateSpeedRDD(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		return inputRDD
				.mapValues(trajectory -> (trajectory.getSpeed() != 0.0 ? trajectory.getSpeed() : null))
				.filter(tuple -> tuple._2() != null);
	}

	/**
	 * Compute acceleration statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 */
	private void generateAccelerationStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		// Create accelerationRDD: (TrajectoryId, TrajectoryAcceleration)
		JavaPairRDD<String, Double> accelerationRDD = inputRDD
				.mapValues(trajectory -> (trajectory.getAcceleration() != 0.0 ? trajectory.getAcceleration() : null))
				.filter(tuple -> tuple._2() != null);

		// Cache accelerationRDD
		accelerationRDD.cache();

		// Compute minAccelerationRDD: (TrajectoryId, MinAcceleration)
		JavaPairRDD<String, Double> minAccelerationRDD = this.computeMinDouble(accelerationRDD);

		// Compute maxAccelerationRDD: (TrajectoryId, MaxSpeed)
		JavaPairRDD<String, Double> maxAccelerationRDD = this.computeMaxDouble(accelerationRDD);

		// Compute avgAccelerationRDD: (TrajectoryId, AvgSpeed)
		JavaPairRDD<String, Double> avgAccelerationRDD = this.computeAverage(accelerationRDD);

		// Compute medianAccelerationRDD: (TrajectoryId, MedianSpeed)
		JavaPairRDD<String, Double> medianAccelerationRDD = this.computeMedian(accelerationRDD);

		// Save acceleration statistics to file:
		minAccelerationRDD.saveAsTextFile(statisticsOutputDir + "/minAccelerationPerTrajectory");
		maxAccelerationRDD.saveAsTextFile(statisticsOutputDir + "/maxAccelerationPerTrajectory");
		avgAccelerationRDD.saveAsTextFile(statisticsOutputDir + "/avgAccelerationPerTrajectory");
		medianAccelerationRDD.saveAsTextFile(statisticsOutputDir + "/medianAccelerationPerTrajectory");
	}

	/**
	 * Compute diff time statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 */
	private void generateDiffTimeStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		// Create timeRDD: (TrajectoryId, DiffTime)
		JavaPairRDD<String, Integer> timeRDD = inputRDD.mapValues(trajectory -> trajectory.getDifftime());
		// Cache timeRDD
		timeRDD.cache();

		// Compute minDiffTimeRDD pro trajectory: (TrajectoryId, MinDiffTime)
		JavaPairRDD<String, Integer> minDiffTimeRDD = this.computeMin(timeRDD);

		// Compute maxDiffTimeRDD pro trajectory: (TrajectoryId, MaxDiffTime)
		JavaPairRDD<String, Integer> maxDiffTimeRDD = this.computeMax(timeRDD);

		// Compute avgDiffTimeRDD: (TrajectoryId, AvgDiffTime)
		JavaPairRDD<String, Double> avgDiffTimeRDD = this
				.computeAverage(timeRDD.mapValues(intVal -> Double.valueOf(intVal)));

		// Compute medianDiffTimeRDD: (TrajectoryId, MedianDiffTime)
		JavaPairRDD<String, Double> medianDiffTimeRDD = this
				.computeMedian(timeRDD.mapValues(intVal -> Double.valueOf(intVal)));

		// Save acceleration statistics to file:
		minDiffTimeRDD.saveAsTextFile(statisticsOutputDir + "/minDiffTimePerTrajectory");
		maxDiffTimeRDD.saveAsTextFile(statisticsOutputDir + "/maxDiffTimePerTrajectory");
		avgDiffTimeRDD.saveAsTextFile(statisticsOutputDir + "/avgDiffTimePerTrajectory");
		medianDiffTimeRDD.saveAsTextFile(statisticsOutputDir + "/medianDiffTimePerTrajectory");
	}

	/**
	 * Compute duration statistics for each trajectory
	 * 
	 * @param inputRDD:
	 *            trajectories RDD
	 */
	private void generateDurationStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		// Create dateRDD: (TrajectoryId, Date)
		JavaPairRDD<String, Long> dateRDD = inputRDD.mapValues(trajectory -> {
			String date = trajectory.getDate1();

			if (trajectory.isPhysicaltime()) {
				return DateTime.parse(date).getMillis();
			} else {
				return Long.valueOf(date);
			}
		});

		// Cache dateRDD
		dateRDD.cache();

		// Compute minDateRDD pro trajectory: (TrajectoryId, MinDate)
		JavaPairRDD<String, Long> minDateRDD = this.computeMin(dateRDD);

		// Compute maxDateRDD pro trajectory: (TrajectoryId, MaxDate)
		JavaPairRDD<String, Long> maxDateRDD = this.computeMax(dateRDD);

		// Save duration statistics to file:
		minDateRDD.saveAsTextFile(statisticsOutputDir + "/minDurationPerTrajectory");
		maxDateRDD.saveAsTextFile(statisticsOutputDir + "/maxDurationPerTrajectory");
	}

	/**
	 * Compute coordinates statistics for each trajectory
	 * 
	 * (bounding rectangle)
	 * 
	 * @param inputRDD:
	 *            trajectories RDD
	 */
	private void generateCoordinatesStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		// Compute xRDD = (TrajectoryId, XCoordinate)
		JavaPairRDD<String, Double> xRDD = inputRDD.mapValues(trajectory -> {
			return trajectory.getX1();
		});

		// Cache xRDD
		xRDD.cache();

		// Compute minXRDD pro trajectory: (TrajectoryId, MinX)
		JavaPairRDD<String, Double> minXRDD = this.computeMinDouble(xRDD);

		// Compute maxXRDD pro trajectory: (TrajectoryId, MaxX)
		JavaPairRDD<String, Double> maxXRDD = this.computeMaxDouble(xRDD);

		// Compute yRDD = (TrajectoryId, yCoordinate)
		JavaPairRDD<String, Double> yRDD = inputRDD.mapValues(trajectory -> {
			return trajectory.getY1();
		});

		// Cache yRDD
		yRDD.cache();

		// Compute minyRDD pro trajectory: (TrajectoryId, MinY)
		JavaPairRDD<String, Double> minYRDD = this.computeMinDouble(yRDD);

		// Compute maxyRDD pro trajectory: (TrajectoryId, MaxY)
		JavaPairRDD<String, Double> maxYRDD = this.computeMaxDouble(yRDD);

		// Save coordinates statistics to file:
		minXRDD.saveAsTextFile(statisticsOutputDir + "/minXPerTrajectory");
		maxXRDD.saveAsTextFile(statisticsOutputDir + "/maxXPerTrajectory");
		minYRDD.saveAsTextFile(statisticsOutputDir + "/minYPerTrajectory");
		maxYRDD.saveAsTextFile(statisticsOutputDir + "/maxYPerTrajectory");
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
	 * Compute min of acceleration or speed or timediff or dates or coordinates
	 * of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff or dates or
	 *            coordinates)
	 * @return pairRDD: (TrajectoryID, Min acceleration or Min speed or Min
	 *         timediff or Min date or Min coordinate)
	 */
	private JavaPairRDD<String, Double> computeMinDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 < v2 ? v1 : v2);
		});
	}

	/**
	 * Compute max of acceleration or speed or timediff or dates or coordinates
	 * of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff or dates or
	 *            coordinates)
	 * @return pairRDD: (TrajectoryID, Max acceleration or Max speed or Max
	 *         timediff or Max date or Max coordinate)
	 */
	private JavaPairRDD<String, Double> computeMaxDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 > v2 ? v1 : v2);
		});
	}

	/**
	 * Compute average of acceleration or speed or timediff of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Avg acceleration or Avg speed or Avg
	 *         timediff)
	 */
	private JavaPairRDD<String, Double> computeAverage(JavaPairRDD<String, Double> pairRDD) {

		final Map<String, Object> nrTraj = pairRDD.countByKey();

		return pairRDD.reduceByKey((v1, v2) -> {
			return v1 + v2;
		}).mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2() / (long) nrTraj.get(tuple._1())));
	}

	/**
	 * Compute median of acceleration or speed or timediff of a trajectory
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Median of acceleration or Median of speed
	 *         or Median of timediff)
	 */
	public JavaPairRDD<String, Double> computeMedian(JavaPairRDD<String, Double> pairRDD) {

		return pairRDD.groupByKey().mapToPair(tuple -> {
			String id = tuple._1();
			ArrayList<Double> difftime = Lists.newArrayList(tuple._2());

			Collections.sort(difftime);

			double median = 0.0;

			if (difftime.size() % 2 == 0) {
				median = ((double) difftime.get(difftime.size() / 2) + (double) difftime.get(difftime.size() / 2 - 1))
						/ 2;
			} else {
				median = (double) difftime.get(difftime.size() / 2);
			}

			return new Tuple2<>(id, median);
		});

	}

	/**
	 *
	 * @param array
	 * @param filename
	 */
	private void writeToFile(Map<String, Object> points) {
		try {

			final File file = new File(statisticsOutputDir + "/pointsPerTrajectory");
			if (!file.exists()) {
				file.createNewFile();
			}

			final FileWriter fw = new FileWriter(file.getAbsoluteFile());
			final BufferedWriter bw = new BufferedWriter(fw);

			for (Map.Entry<String, Object> point : points.entrySet()) {
				bw.append("Trajectory: " + point.getKey() + "NPoints: " + point.getValue());
				bw.newLine();
			}

			bw.close();

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
