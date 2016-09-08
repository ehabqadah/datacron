package de.fhg.iais.kd.datacron.trajectories.cleaning.guice;

import com.google.common.base.Joiner;

import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBStatistics;
import de.fhg.iais.kd.datacron.trajectories.cleaning.table.beans.TBTrajectories;

/**
 * @author kthellmann
 */
public class ClassRegister {

	public static final String TABLE_BEANS_REGISTER = Joiner.on(",")//
			.join(//
					TBStatistics.class.getName(), //
					TBTrajectories.class.getName()
	);
}
