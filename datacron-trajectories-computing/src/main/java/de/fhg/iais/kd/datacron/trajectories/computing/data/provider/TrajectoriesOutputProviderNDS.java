package de.fhg.iais.kd.datacron.trajectories.computing.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesOutput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.metadata.TBMTrajectoriesOutputNDS;

public class TrajectoriesOutputProviderNDS extends AbstractDataProvider<TBTrajectoriesOutput> {

	private static final long serialVersionUID = 3042437178960616961L;

	@Inject
	public TrajectoriesOutputProviderNDS( //
			TBMTrajectoriesOutputNDS tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBTrajectoriesOutput.class);
	}

}
