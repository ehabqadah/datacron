package de.fhg.iais.kd.datacron.trajectories.computing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.trajectories.computing.data.provider.TrajectoriesStatisticsOutputProvider;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesOutput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesStatisticsOutput;
import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
public class Statistics implements Serializable {

	private static final long serialVersionUID = 5418827747835053055L;

	@Inject
	private transient TrajectoriesStatisticsOutputProvider statisticsOutputProvider;

	@Inject
	@Named("spark.app.trajectories.statistics")
	private String statisticsOutputDir;

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
		final JavaPairRDD<String, TBTrajectoriesOutput> inputRDD = trajectoriesRDD.keyBy(trajectory -> trajectory.getId());

		// Cache pair1RDD
		inputRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

		// 0. Compute n points for each trajectory
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics1RDD = this.generatePointStatistics(inputRDD);

		// 1. Compute speed statistics for each trajectory:
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = this.generateSpeedStatistics(inputRDD, statistics1RDD);

		// 2. Compute acceleration statistics for each trajectory:
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = this.generateAccelerationStatistics(inputRDD, statistics2RDD);

		// 3. Compute diff time statistics for each trajectory:
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics4RDD = this.generateDiffTimeStatistics(inputRDD, statistics3RDD);

		// 4. Compute duration statistics for each trajectory:
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics5RDD = this.generateDurationStatistics(inputRDD, statistics4RDD);

		// 5. Compute coordinates statistics for each trajectory:
		final JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics6RDD = this.generateCoordinatesStatistics(inputRDD, statistics5RDD);
		
		this.statisticsOutputProvider.updateTable(statistics6RDD.values());
	}

	/**
	 * Nr of points for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generatePointStatistics(
			JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		final Map<String, Object> points = inputRDD.countByKey();

		ImmutableList.Builder<TBTrajectoriesStatisticsOutput> statisticsOutput = ImmutableList.builder();

		for (Entry<String, Object> entry : points.entrySet()) {
			TBTrajectoriesStatisticsOutput statistics = new TBTrajectoriesStatisticsOutput();
			statistics.setId(entry.getKey());
			statistics.setNr_points((int) entry.getValue());
			statisticsOutput.add(statistics);
		}

		JavaRDD<TBTrajectoriesStatisticsOutput> statisticsOutputRDD = this.statisticsOutputProvider.getSparkContext()
				.parallelize(statisticsOutput.build());

		return statisticsOutputRDD.keyBy((TBTrajectoriesStatisticsOutput arg0) -> {
			return arg0.getId();
		});

	}

	/**
	 * Compute speed statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return 
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generateSpeedStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD, JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statisticsRDD) {
		// Create speedRDD: (TrajectoryId, TrajectorySpeed)
		final JavaPairRDD<String, Double> speedRDD = calculateSpeedRDD(inputRDD);

		// Cache speedRDD
		speedRDD.cache();

		// Compute minSpeedRDD: (TrajectoryId, MinSpeed)
		final JavaPairRDD<String, Double> minSpeedRDD = this.computeMinDouble(speedRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = minSpeedRDD.join(statisticsRDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double minspeed = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_speed(minspeed);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxSpeedRDD: (TrajectoryId, MaxSpeed)
		final JavaPairRDD<String, Double> maxSpeedRDD = this.computeMaxDouble(speedRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = maxSpeedRDD.join(statistics2RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double maxspeed = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_speed(maxspeed);

			return new Tuple2<>(id, statistics);
		});
		
		// Compute avgSpeedRDD: (TrajectoryId, AvgSpeed)
		final JavaPairRDD<String, Double> avgSpeedRDD = this.computeAverage(speedRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics4RDD = avgSpeedRDD.join(statistics3RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double avgspeed = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setAvg_speed(avgspeed);

			return new Tuple2<>(id, statistics);
		});

		// Compute medianSpeedRDD: (TrajectoryId, MedianSpeed)
		final JavaPairRDD<String, Double> medianSpeedRDD = this.computeMedian(speedRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics5RDD = medianSpeedRDD.join(statistics4RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double medianspeed = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setAvg_speed(medianspeed);

			return new Tuple2<>(id, statistics);
		});
		
		return statistics5RDD;
	}

	public JavaPairRDD<String, Double> calculateSpeedRDD(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD) {
		return inputRDD.mapValues(trajectory -> (trajectory.getSpeed() != 0.0 ? trajectory.getSpeed() : null))
				.filter(tuple -> tuple._2() != null);
	}

	/**
	 * Compute acceleration statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return 
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generateAccelerationStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD, JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statisticsRDD) {
		// Create accelerationRDD: (TrajectoryId, TrajectoryAcceleration)
		final JavaPairRDD<String, Double> accelerationRDD = inputRDD
				.mapValues(trajectory -> (trajectory.getAcceleration() != 0.0 ? trajectory.getAcceleration() : null))
				.filter(tuple -> tuple._2() != null);

		// Cache accelerationRDD
		accelerationRDD.cache();

		// Compute minAccelerationRDD: (TrajectoryId, MinAcceleration)
		final JavaPairRDD<String, Double> minAccelerationRDD = this.computeMinDouble(accelerationRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = minAccelerationRDD.join(statisticsRDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double minacceleration = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_acceleration(minacceleration);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxAccelerationRDD: (TrajectoryId, MaxSpeed)
		final JavaPairRDD<String, Double> maxAccelerationRDD = this.computeMaxDouble(accelerationRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = maxAccelerationRDD.join(statistics2RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double maxacceleration = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_acceleration(maxacceleration);

			return new Tuple2<>(id, statistics);
		});
		
		// Compute avgAccelerationRDD: (TrajectoryId, AvgSpeed)
		final JavaPairRDD<String, Double> avgAccelerationRDD = this.computeAverage(accelerationRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics4RDD = avgAccelerationRDD.join(statistics3RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double avgacceleration = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setAvg_acceleration(avgacceleration);

			return new Tuple2<>(id, statistics);
		});
		
		// Compute medianAccelerationRDD: (TrajectoryId, MedianSpeed)
		final JavaPairRDD<String, Double> medianAccelerationRDD = this.computeMedian(accelerationRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics5RDD = medianAccelerationRDD.join(statistics4RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			Double medianacceleration = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMedian_acceleration(medianacceleration);

			return new Tuple2<>(id, statistics);
		});

		return statistics5RDD;
		
	}

	/**
	 * Compute diff time statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return 
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generateDiffTimeStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD, JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statisticsRDD) {
		// Create timeRDD: (TrajectoryId, DiffTime)
		final JavaPairRDD<String, Integer> timeRDD = inputRDD.mapValues(trajectory -> trajectory.getDifftime());
		// Cache timeRDD
		timeRDD.cache();

		// Compute minDiffTimeRDD pro trajectory: (TrajectoryId, MinDiffTime)
		final JavaPairRDD<String, Integer> minDiffTimeRDD = this.computeMin(timeRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = minDiffTimeRDD.join(statisticsRDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double mindifftime = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_difftime(mindifftime);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxDiffTimeRDD pro trajectory: (TrajectoryId, MaxDiffTime)
		final JavaPairRDD<String, Integer> maxDiffTimeRDD = this.computeMax(timeRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = maxDiffTimeRDD.join(statistics2RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double maxdifftime = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_difftime(maxdifftime);

			return new Tuple2<>(id, statistics);
		});

		// Compute avgDiffTimeRDD: (TrajectoryId, AvgDiffTime)
		final JavaPairRDD<String, Double> avgDiffTimeRDD = this.computeAverage(timeRDD.mapValues(intVal -> Double.valueOf(intVal)));
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics4RDD = avgDiffTimeRDD.join(statistics3RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double avgdifftime = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setAvg_difftime(avgdifftime);

			return new Tuple2<>(id, statistics);
		});

		// Compute medianDiffTimeRDD: (TrajectoryId, MedianDiffTime)
		final JavaPairRDD<String, Double> medianDiffTimeRDD = this.computeMedian(timeRDD.mapValues(intVal -> Double.valueOf(intVal)));
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics5RDD = medianDiffTimeRDD.join(statistics4RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double mediandifftime = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMedian_difftime(mediandifftime);

			return new Tuple2<>(id, statistics);
		});
		
		return statistics5RDD;
	}

	/**
	 * Compute duration statistics for each trajectory
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return 
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generateDurationStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD, JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statisticsRDD) {
		// Create dateRDD: (TrajectoryId, Date)
		final JavaPairRDD<String, Long> dateRDD = inputRDD.mapValues(trajectory -> {
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
		final JavaPairRDD<String, Long> minDateRDD = this.computeMin(dateRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = minDateRDD.join(statisticsRDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double mindate = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_date(mindate);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxDateRDD pro trajectory: (TrajectoryId, MaxDate)
		final JavaPairRDD<String, Long> maxDateRDD = this.computeMax(dateRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = maxDateRDD.join(statistics2RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double maxdate = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_date(maxdate);

			return new Tuple2<>(id, statistics);
		});

		return statistics3RDD;
	}

	/**
	 * Compute coordinates statistics for each trajectory
	 * 
	 * (bounding rectangle)
	 * 
	 * @param inputRDD: trajectories RDD
	 * @return 
	 */
	private JavaPairRDD<String, TBTrajectoriesStatisticsOutput> generateCoordinatesStatistics(JavaPairRDD<String, TBTrajectoriesOutput> inputRDD, JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statisticsRDD) {
		// Compute xRDD = (TrajectoryId, XCoordinate)
		final JavaPairRDD<String, Double> xRDD = inputRDD.mapValues(trajectory -> {
			return trajectory.getX1();
		});

		// Cache xRDD
		xRDD.cache();

		// Compute minXRDD pro trajectory: (TrajectoryId, MinX)
		final JavaPairRDD<String, Double> minXRDD = this.computeMinDouble(xRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics2RDD = minXRDD.join(statisticsRDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double minx = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_X(minx);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxXRDD pro trajectory: (TrajectoryId, MaxX)
		final JavaPairRDD<String, Double> maxXRDD = this.computeMaxDouble(xRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics3RDD = maxXRDD.join(statistics2RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double maxx = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_X(maxx);

			return new Tuple2<>(id, statistics);
		});
		
		// Compute yRDD = (TrajectoryId, yCoordinate)
		final JavaPairRDD<String, Double> yRDD = inputRDD.mapValues(trajectory -> {
			return trajectory.getY1();
		});

		// Cache yRDD
		yRDD.cache();

		// Compute minyRDD pro trajectory: (TrajectoryId, MinY)
		final JavaPairRDD<String, Double> minYRDD = this.computeMinDouble(yRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics4RDD = minYRDD.join(statistics3RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double miny = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMin_Y(miny);

			return new Tuple2<>(id, statistics);
		});

		// Compute maxyRDD pro trajectory: (TrajectoryId, MaxY)
		final JavaPairRDD<String, Double> maxYRDD = this.computeMaxDouble(yRDD);
		JavaPairRDD<String, TBTrajectoriesStatisticsOutput> statistics5RDD = maxYRDD.join(statistics4RDD).mapToPair(arg0 -> {
			String id = arg0._1();
			double maxy = arg0._2()._1();
			TBTrajectoriesStatisticsOutput statistics = arg0._2()._2();
			statistics.setMax_Y(maxy);

			return new Tuple2<>(id, statistics);
		});

		return statistics5RDD;
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
	 * @param pairRDD: (Table, dates or coordinates)
	 * @return pairRDD: (Table, Max date or Max coordinate)
	 */
	private <T extends Comparable<T>> JavaPairRDD<String, T> computeMax(JavaPairRDD<String, T> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1.compareTo(v2) > 0 ? v1 : v2);
		});
	}

	/**
	 * Compute min of acceleration or speed or timediff or dates or coordinates of a trajectory
	 * 
	 * @param pairRDD:(TrajectoryID, acceleration or speed or timediff or dates or coordinates)
	 * @return pairRDD: (TrajectoryID, Min acceleration or Min speed or Min timediff or Min date or Min coordinate)
	 */
	private JavaPairRDD<String, Double> computeMinDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 < v2 ? v1 : v2);
		});
	}

	/**
	 * Compute max of acceleration or speed or timediff or dates or coordinates of a trajectory
	 * 
	 * @param pairRDD:(TrajectoryID, acceleration or speed or timediff or dates or coordinates)
	 * @return pairRDD: (TrajectoryID, Max acceleration or Max speed or Max timediff or Max date or Max coordinate)
	 */
	private JavaPairRDD<String, Double> computeMaxDouble(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.reduceByKey((v1, v2) -> {
			return (v1 > v2 ? v1 : v2);
		});
	}

	/**
	 * Compute average of acceleration or speed or timediff of a trajectory
	 * 
	 * @param pairRDD:(TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Avg acceleration or Avg speed or Avg timediff)
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
	 * @param pairRDD:(TrajectoryID, acceleration or speed or timediff)
	 * @return pairRDD: (TrajectoryID, Median of acceleration or Median of speed or Median of timediff)
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
}