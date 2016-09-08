package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public class TBMTrajectories implements ITableMetadata {

	private static final long serialVersionUID = -4231611149506441800L;
	
	@Inject
	@Named("spark.app.trajectories")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories input or output.";
	
	private static final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id", "text") //
			.build();
	
	private static final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.put("id_c", "int") //
			.build();
	
	private static final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //
			.put("date1", "text") //
			.put("date2", "text") //
			.put("difftime", "int") //
			.put("x1", "double") //
			.put("x2", "double") //
			.put("diffx", "double") //
			.put("y1", "double") //
			.put("y2", "double") //
			.put("diffy", "double") //
			.put("distance", "double") //
			.put("speed", "double") //
			.put("course", "double") //
			.put("acceleration", "double") //
			.put("turn", "double") //
			.put("abs_prop", "map<text, double>") //
			.put("rel_prop", "map<text, double>") //
			.build(); //

	public ImmutableMap<String, String> getPartitionKeys() {
		return partitionKeys;
	}

	public ImmutableMap<String, String> getClusterKeys() {
		return clusterKeys;
	}

	public ImmutableMap<String, String> getColumns() {
		return columns;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
