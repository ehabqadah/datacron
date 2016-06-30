package de.fhg.iais.generated.table.columns.metadata;

import com.google.common.collect.ImmutableMap;

/**
 * Items Table Bean
 * 
 * @author kthellmann
 *
 */
public class TBMItems implements IColumnMetadata {
	private static final long serialVersionUID = 8173811584155934968L;

	private String tableName = "items";

	private final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id", "text") //
			.build();
	
	private final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.build();
		
	private final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //
			.put("id", "text") //
			.put("content", "blob") //			
			.put("ingestdate", "timestamp") //
			.build();

	@Override
	public String getTableName() {
		return this.tableName;
	}

	@Override
	public ImmutableMap<String, String> getPartitionKeys() {
		return this.partitionKeys;
	}

	@Override
	public ImmutableMap<String, String> getClusterKeys() {
		return this.clusterKeys;
	}

	@Override
	public ImmutableMap<String, String> getColumns() {
		return this.columns;
	}
}
