package de.fhg.iais.kd.datacron.trajectories.cleaning.data.provider;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.cassandra.AbstractDataProvider;
import de.fhg.iais.cassandra.Cassandra;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBInputTrajectory;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata.TBMInputTrajectory;

public class TrajectoriesProvider extends AbstractDataProvider<TBInputTrajectory> {

	private static final long serialVersionUID = -5556975391499283873L;

	@Inject
	@Named("spark.app.trajectories.physicaltime")
	private boolean timeIsPhysical;

	@Inject
	@Named("spark.app.trajectories.geocoordinates")
	private boolean coordinatesAreGeo;


	@Inject
	public TrajectoriesProvider( //
			TBMInputTrajectory tableMetadata, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra) {

		super(tableMetadata, sparkContext, cassandra, TBInputTrajectory.class);
	}

	public boolean isTimeIsPhysical() {
		return timeIsPhysical;
	}

	public boolean isCoordinatesAreGeo() {
		return coordinatesAreGeo;
	}
}
