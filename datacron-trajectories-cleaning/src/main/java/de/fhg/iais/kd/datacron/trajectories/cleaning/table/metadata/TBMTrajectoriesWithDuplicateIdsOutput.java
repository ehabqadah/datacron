package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 */
public class TBMTrajectoriesWithDuplicateIdsOutput extends TBMTrajectories {
	
	private static final long serialVersionUID = -4886696149322240100L;

	@Inject
	@Named("spark.app.trajectories.with.duplicate.ids.output")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories with duplicate ids output.";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
