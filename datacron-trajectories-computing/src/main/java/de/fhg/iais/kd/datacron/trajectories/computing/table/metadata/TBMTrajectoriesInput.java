package de.fhg.iais.kd.datacron.trajectories.computing.table.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public class TBMTrajectoriesInput implements ITableMetadata {

	private static final long serialVersionUID = -2201479500537234350L;

	@Inject
	@Named("spark.app.trajectories.inputdata")
	private String tableName;
	private static final String DESCRIPTION = "";

	private static final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id_c", "bigint")//
			.build();

	private static final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.build();

	private static final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //
			.put("id", "text")//
			.put("d", "text")//
			.put("x", "text")//
			.put("y", "text")//
			.put("additional", "frozen<map<text,text>>")//
			.build(); //

	public String getTableName() {
		return tableName;
	}

	public ImmutableMap<String, String> getPartitionKeys() {
		return partitionKeys;
	}

	public ImmutableMap<String, String> getClusterKeys() {
		return clusterKeys;
	}

	public ImmutableMap<String, String> getColumns() {
		return columns;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

}
