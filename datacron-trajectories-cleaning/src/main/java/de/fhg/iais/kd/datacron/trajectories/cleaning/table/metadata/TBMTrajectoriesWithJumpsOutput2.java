package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithJumpsOutput2 extends TBMOutlierTrajectory {
	
	private static final long serialVersionUID = 5464159792604988807L;

	@Inject
	@Named("spark.app.trajectories.with.jumps.dist.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories with jumps output. (Distance based metric)";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
