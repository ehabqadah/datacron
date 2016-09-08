package de.fhg.iais.kd.datacron.trajectories.computing.guice;

import com.google.common.base.Joiner;

import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesOutput;
import de.fhg.iais.kd.datacron.trajectories.computing.table.beans.TBTrajectoriesParametrizedInput;

/**
 * @author kthellmann
 *
 */
public class ClassRegister {

	public static final String TABLE_BEANS_REGISTER = Joiner.on(",")//
			.join(//
					TBTrajectoriesParametrizedInput.class.getName(), //
					TBTrajectoriesOutput.class.getName()//
	);
}
