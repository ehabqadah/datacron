package de.fhg.iais.cassandra;

import java.io.Serializable;

import com.google.common.collect.ImmutableMap;

/**
 * @author kthellmann
 *
 */
public interface ITableMetadata extends Serializable {
	
	public String getTableName();
	
	public String getDescription();

	public ImmutableMap<String, String> getPartitionKeys();

	public ImmutableMap<String, String> getClusterKeys();

	public ImmutableMap<String, String> getColumns();
	
}
