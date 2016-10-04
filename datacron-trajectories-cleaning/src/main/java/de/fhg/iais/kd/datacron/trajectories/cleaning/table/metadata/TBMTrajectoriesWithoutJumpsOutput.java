package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithoutJumpsOutput extends TBMOutputTrajectory {
	
	private static final long serialVersionUID = 869272068978779044L;

	@Inject
	@Named("spark.app.trajectories.without.jumps.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories without jumps output.";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
