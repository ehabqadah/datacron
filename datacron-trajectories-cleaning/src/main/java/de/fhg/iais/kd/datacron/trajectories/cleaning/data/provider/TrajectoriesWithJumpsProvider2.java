package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBOutlierTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMTrajectoriesWithJumpsOutput2;

/**
 * @author kthellmann
 */
public class TrajectoriesWithJumpsProvider2 extends AbstractDataProvider<TBOutlierTrajectory> {

	private static final long serialVersionUID = 7066653900442280347L;

	@Inject
	public TrajectoriesWithJumpsProvider2( //
			TBMTrajectoriesWithJumpsOutput2 tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBOutlierTrajectory.class);
	}

}
