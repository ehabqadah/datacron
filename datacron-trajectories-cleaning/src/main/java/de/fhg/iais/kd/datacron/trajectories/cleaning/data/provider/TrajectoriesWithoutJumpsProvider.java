package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithoutJumpsOutput;

/**
 * @author kthellmann
 */
public class TrajectoriesWithoutJumpsProvider extends AbstractDataProvider<TBOutputTrajectory> {

	private static final long serialVersionUID = -5556975391499283873L;

	@Inject
	public TrajectoriesWithoutJumpsProvider( //
			TBMTrajectoriesWithoutJumpsOutput tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutputTrajectory.class);
	}

}
