package de.fhg.iais.kd.datacron.trajectories.cleaning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.StatisticsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider.TrajectoriesWithDuplicateIdsProvider;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBStatistics;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBTrajectories;
import scala.Tuple2;

/**
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
	@Named("spark.app.trajectories.speedoutlierthresholdfactor")
	private double speedThresholdFactor;

	/**
	 * Detect and correct corrupt trajectories:
	 * 
	 * I. Characteristics:
	 *  > Non-unique ID
	 *  > Time intervals overlapping
	 *   
	 * II. Detection criteria: 
	 *  > Exceptional high speed median
	 */
	public void clean() {		
		this.detect();
 	}
	
	/**
	 * Detect corrupt trajectories.
	 */
	private void detect(){
		// Load computed trajectories 
		final JavaRDD<TBTrajectories> inputRDD = trajectoriesInputProvider.readInputTableRows();
		
		// Load computed statistics about trajectories
		final JavaRDD<TBStatistics> statisticsRDD = statisticsInputProvider.readInputTableRows();

		// Create speedMedianRDD: (TrajectoryId, TrajectorySpeedMedian)
		// TrajectorySpeedMedian: speed median m_ti for each trajectory i€ {1..n}
		final JavaPairRDD<String, Double> speedMedianRDD = statisticsRDD.mapToPair(arg0 -> {
			return new Tuple2<>(arg0.getId(), arg0.getMedian_speed());
		});
		
		// Compute median m of all m_ti
		final double speedMedian = this.computeMedian(speedMedianRDD.mapToPair(pair -> new Tuple2<>("ALL", pair._2()))).first()._2();
		
		// Set threshold th = c * m (c constant)
		final double speedThreshold = speedThresholdFactor * speedMedian;
		
		// Compare all m_ti with m: if (m_ti > th) then possible outlier (= trajectory with duplicate ID)
		final JavaPairRDD<String, Double> speedOutlierRDD = speedMedianRDD.filter(tuple -> tuple._2() > speedThreshold);
		final List<String> speedOutlier = speedOutlierRDD.keys().collect();
		
		// Filter trajectories with duplicate ids
		final JavaRDD<TBTrajectories> trajectoriesWithDuplicateIDsRDD = inputRDD.filter( arg0 -> { return speedOutlier.contains(arg0.getId()); });
		
		// Cache RDD
		trajectoriesWithDuplicateIDsRDD.cache();
		
		// Update table of trajectories with duplicate ids
		trajectoriesWithDuplicateIdsProvider.updateTable(trajectoriesWithDuplicateIDsRDD);
		
		// Fix corrupt trajectory
		this.correct(trajectoriesWithDuplicateIDsRDD);
	}
	
	/**
	 * Correct corrupt trajectories.
	 */
	private void correct(JavaRDD<TBTrajectories> trajectoriesWithDuplicateIDsRDD){		
	}	
	
	/**
	 * Compute speed median
	 * 
	 * @param pairRDD: (TrajectoryID, speed)
	 * @return pairRDD: (TrajectoryID, speed median)
	 */
	private JavaPairRDD<String, Double> computeMedian(JavaPairRDD<String, Double> pairRDD) {
		return pairRDD.groupByKey().mapToPair(tuple -> {
			final String id = tuple._1();
			final ArrayList<Double> difftime = Lists.newArrayList(tuple._2());
			Collections.sort(difftime);
			double median = 0.0;

			if (difftime.size() % 2 == 0) {
				median = ((double) difftime.get(difftime.size() / 2) + (double) difftime.get(difftime.size() / 2 - 1)) / 2;
			} else {
				median = (double) difftime.get(difftime.size() / 2);
			}

			return new Tuple2<>(id, median);
		});
	}	

}
