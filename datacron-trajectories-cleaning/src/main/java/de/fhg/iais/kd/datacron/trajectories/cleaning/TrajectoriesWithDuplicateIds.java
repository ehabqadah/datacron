package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.common.utils.Utils;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.StatisticsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithDuplicateIdsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithoutDuplicateIdsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBInputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutlierTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBStatistics;
import scala.Tuple2;

/**
 * Detect and correct trajectories with duplicate ids:
 * 
 * I. Characteristics: Non-unique ID. Time intervals overlapping.
 * 
 * II. Detection criteria: Exceptional high speed median
 * 
 * III. Correction approach: Determine distance threshold of tx:Compute min
 * spanning tree (edge=distance btw. points in chronological order ). Compute
 * median m* of distances (edges of spanning tree) (Idea: Median might help in
 * determining the minimal erroneous connection btw. 2 points). Determine
 * largest distance d* btw 2 points of tx.Compute threshold_tx for separating tx
 * into tx1 and tx2: (m* + d*)/2 (Idea: Average of min and max erronous
 * connection of tx). Perform splitting for each corrupt trajectory of type dID
 * by using threshold_tx.
 * 
 * @author kthellmann
 */
@Singleton
public class TrajectoriesWithDuplicateIds implements Serializable {

	private static final long serialVersionUID = 2103621255207851003L;

	@Inject
	private transient TrajectoriesProvider trajectoriesInputProvider;
	@Inject
	private transient StatisticsProvider statisticsInputProvider;
	@Inject
	private transient TrajectoriesWithDuplicateIdsProvider trajectoriesWithDuplicateIdsProvider;
	@Inject
	private transient TrajectoriesWithoutDuplicateIdsProvider trajectoriesWithoutDuplicateIdsProvider;

	@Inject
	@Named("spark.app.trajectories.dupid.th.factor")
	private double dupIdThresholdFactor;

	/**
	 * Detect and correct trajectories with duplicate id
	 */
	public void clean() {
		this.detect();
	}

	/**
	 * Detect trajectories with duplicate ids
	 */
	private void detect() {
		// Load computed trajectories
		final JavaRDD<TBInputTrajectory> inputRDD = trajectoriesInputProvider.readInputTableRows();

		// Load computed statistics about trajectories
		final JavaRDD<TBStatistics> statisticsRDD = statisticsInputProvider.readInputTableRows();

		// Create speedMedianRDD: (TrajectoryId, m_ti)
		// Speed median m_ti for each trajectory i€ {1..n}
		final JavaPairRDD<String, Double> speedMedianRDD = statisticsRDD.mapToPair(arg0 -> {
			return new Tuple2<>(arg0.getId(), arg0.getMedian_speed());
		});

		// Compute median m of all m_ti
		final double speedMedian = this.computeMedian(speedMedianRDD.mapToPair(pair -> new Tuple2<>("ALL", pair._2()))).first()._2();

		// Set threshold th = c * m (c constant)
		final double speedThreshold = dupIdThresholdFactor * speedMedian;

		// Compare all m_ti with m: if (m_ti > th) then possible outlier
		final JavaPairRDD<String, Double> speedOutlierRDD = speedMedianRDD.filter(tuple -> tuple._2() > speedThreshold);
		final Set<String> speedOutlier = new HashSet<>(speedOutlierRDD.keys().collect());

		// Filter trajectories with duplicate ids
		final JavaRDD<TBOutlierTrajectory> trajectoriesWithDuplicateIDsRDD = inputRDD.filter(arg0 -> {
			if (speedOutlier.contains(arg0.getId())) {				
				return true;
			}

			return false;
			
		}).map(arg0 -> {
			boolean outlier = false;
			
			if (arg0.getSpeed() > speedThreshold) {
				outlier = true;
			}	
			
			Map<String, Double> additional = new HashMap<String, Double>();
			additional.putAll(arg0.getAbs_prop());
			additional.putAll(arg0.getRel_prop());
			
			return new TBOutlierTrajectory(//
					arg0.getId(), //
					arg0.getId_c(), //
					arg0.getDate1(), //
					arg0.getX1(), //
					arg0.getY1(), //
					arg0.getX2(), //
					arg0.getY2(), //
					arg0.getSpeed(), //
					additional, //
					outlier);
		});

		// Cache RDD
		trajectoriesWithDuplicateIDsRDD.cache();

		// Update table of trajectories with duplicate ids
		trajectoriesWithDuplicateIdsProvider.updateTable(trajectoriesWithDuplicateIDsRDD);

//		System.out.println("TRAJECTORIES WITH DUPLICATE IDs: " + trajectoriesWithDuplicateIDsRDD.collect());

		// Fix corrupt trajectory
		this.correct(trajectoriesWithDuplicateIDsRDD);
	}

	/**
	 * Correct trajectories with duplicate ids
	 */
	private void correct(JavaRDD<TBOutlierTrajectory> trajectoriesWithDuplicateIDsRDD) {

		JavaRDD<TBOutputTrajectory> splitTrajectoriesRDD = trajectoriesWithDuplicateIDsRDD
				.groupBy(point -> point.getId()) //
				.flatMap(v1 -> {
					String idTx = v1._1();
					Iterable<TBOutlierTrajectory> trajectoryTxPoints = v1._2();

					// I. Determine distance threshold of tx:
					final boolean isPhysicalTime = trajectoriesInputProvider.isTimeIsPhysical();
					final boolean areGeoCoordinates = trajectoriesInputProvider.isCoordinatesAreGeo();

					// Sort trajectory points chronological
					ImmutableList<TBOutlierTrajectory> chronologicalPoints = sortChronologically(trajectoryTxPoints,
							isPhysicalTime);

					List<Double> minimumDistances = new ArrayList<>();
					double maxErrDist = Double.MIN_VALUE;

					// Compute distance from point x to all other points:
					// -> Min distance (= min spanning tree) for median
					// -> Max distance for average
					for (int i = 0; i < chronologicalPoints.size() - 1; i++) {
						double minimumDistance = Double.MAX_VALUE;
						TBOutlierTrajectory point1 = chronologicalPoints.get(i);
						double x1 = point1.getX1();
						double y1 = point1.getY1();

						for (int j = i + 1; j < chronologicalPoints.size(); j++) {
							TBOutlierTrajectory point2 = chronologicalPoints.get(j);
							double x2 = point2.getX1();
							double y2 = point2.getY1();

							double distance = Utils.calculateDistance(areGeoCoordinates, x1, y1, x2, y2);

							if (distance < minimumDistance) {
								minimumDistance = distance;
							}

							if (distance > maxErrDist) {
								maxErrDist = distance;
							}
						}

						minimumDistances.add(minimumDistance);
					}

					// Compute median of all min distances of tx.
					// Median might help in determining the min erroneous
					// connection btw. 2 points.
					double minErrDist = Utils.computeMedian(minimumDistances);

					// Distance threshold for tx.
					double thresholdTx = (maxErrDist + minErrDist) / 2;

					// II. Perform splitting of tx into tx1 and tx2
					Set<TBOutputTrajectory> splitTrajectories = new HashSet<TBOutputTrajectory>();
					String idTx2 = String.valueOf(Math.random() * Integer.valueOf(idTx));

					TBOutlierTrajectory current = chronologicalPoints.iterator().next();

					for (int i = 1; i < chronologicalPoints.size(); i++) {
						double x1 = current.getX1();
						double y1 = current.getY1();

						TBOutlierTrajectory next = chronologicalPoints.get(i);
						double x2 = next.getX1();
						double y2 = next.getY1();

						double distanceCurrentNext = Utils.calculateDistance(areGeoCoordinates, x1, y1, x2, y2);

						if (distanceCurrentNext < thresholdTx) {
							TBOutputTrajectory trajectoryTx1 = new TBOutputTrajectory(//
									idTx, next.getId_c(), next.getDate1(), String.valueOf(next.getX1()),
									String.valueOf(next.getX2()), next.getAdditional());

							splitTrajectories.add(trajectoryTx1);

						} else {
							TBOutputTrajectory trajectoryTx2 = new TBOutputTrajectory(//
									idTx2, next.getId_c(), next.getDate1(), String.valueOf(next.getX1()),
									String.valueOf(next.getX2()), next.getAdditional());

							splitTrajectories.add(trajectoryTx2);
						}
					}

					return splitTrajectories;
				});

		splitTrajectoriesRDD.cache();

//		System.out.println("SPLIT TRAJECTORIES: " + splitTrajectoriesRDD.collect());

		trajectoriesWithoutDuplicateIdsProvider.updateTable(splitTrajectoriesRDD);

	}

	private static ImmutableList<TBOutlierTrajectory> sortChronologically(Iterable<TBOutlierTrajectory> trajectoryTxPoints,
			final boolean isPhysicalTime) {
		return Ordering.from((TBOutlierTrajectory point1, TBOutlierTrajectory point2) -> {
			long time1 = Utils.calculateTimeFromString(isPhysicalTime, point1.getDate1());
			long time2 = Utils.calculateTimeFromString(isPhysicalTime, point2.getDate1());

			return Long.compare(time1, time2);

		}).immutableSortedCopy(trajectoryTxPoints);
	}

	/**
	 * Compute speed median
	 * 
	 * @param pairRDD:
	 *            (TrajectoryID, speed)
	 * @return pairRDD: (TrajectoryID, speed median)
	 */
	private JavaPairRDD<String, Double> computeMedian(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.groupByKey().mapToPair(tuple -> {
			final String id = tuple._1();
			final ArrayList<Double> difftime = Lists.newArrayList(tuple._2());
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
