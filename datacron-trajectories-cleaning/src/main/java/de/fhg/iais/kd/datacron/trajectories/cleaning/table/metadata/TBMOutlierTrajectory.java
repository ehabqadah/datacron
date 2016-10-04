package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.common.collect.ImmutableMap;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public abstract class TBMOutlierTrajectory implements ITableMetadata {

	private static final long serialVersionUID = 8851149519594348772L;

	private String DESCRIPTION = "";

	private static final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id", "text")//
			.build();
	
	private static final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.put("id_c", "int")//
			.build();
	
	private static final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //		
			.put("date1", "text")//
			.put("x1", "double")//
			.put("y1", "double")//
			.put("x2", "double")//
			.put("y2", "double")//
			.put("speed", "double")//
			.put("additional", "frozen<map<text,double>>")//
			.put("outlier", "boolean")//
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
