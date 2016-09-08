package de.fhg.iais.kd.datacron.trajectories.computing.table.metadata;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * @author kthellmann
 *
 */
public class TBMTrajectoriesOutputNDS extends TBMTrajectoriesOutputND {
	private static final long serialVersionUID = -4522163764638199250L;

	@Inject
	@Named("spark.app.trajectories.output.nds")
	private String tableName;
	private static final String DESCRIPTION = "trajectories table - stationary points";

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
