package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithoutJumpsOutput2;

/**
 * @author kthellmann
 */
public class TrajectoriesWithoutJumpsProvider2 extends AbstractDataProvider<TBOutputTrajectory> {

	private static final long serialVersionUID = -754224956308471709L;

	@Inject
	public TrajectoriesWithoutJumpsProvider2( //
			TBMTrajectoriesWithoutJumpsOutput2 tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutputTrajectory.class);
	}

}
