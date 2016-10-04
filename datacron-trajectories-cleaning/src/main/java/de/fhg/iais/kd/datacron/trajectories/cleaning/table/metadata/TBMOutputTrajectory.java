package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.common.collect.ImmutableMap;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public abstract class TBMOutputTrajectory implements ITableMetadata {

	private static final long serialVersionUID = 6498474011903806337L;
	
	private String DESCRIPTION = "";

	private static final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id", "text")//
			.build();
	
	private static final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.put("id_c", "int")//
			.build();
	
	private static final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //		
			.put("d", "text")//
			.put("x", "text")//
			.put("y", "text")//
			.put("additional", "frozen<map<text,text>>")//
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

	public String getDescription() {
		return DESCRIPTION;
	}
}
