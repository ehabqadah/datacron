package de.fhg.iais.kd.datacron.files.importer.table.metadata;

import com.google.common.collect.ImmutableMap;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public class TBMInput implements ITableMetadata {

	private static final long serialVersionUID = -2201479500537234350L;

	private static final String TABLE_NAME = "";
	private static final String DESCRIPTION = "";

	public final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.build();
	
	public final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.build();
	
	public final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //		
			.build(); //

	public String getTableName() {
		return TABLE_NAME;
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

	public String getDescription() {
		return DESCRIPTION;
	}

	


}
