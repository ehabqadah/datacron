package de.fhg.iais.generated.table.columns.metadata;
			
import java.io.Serializable;

import com.google.common.collect.ImmutableMap;
			
public interface IColumnMetadata extends Serializable {

	public String getTableName();

	public ImmutableMap<String, String> getPartitionKeys();

	public ImmutableMap<String, String> getClusterKeys();

	public ImmutableMap<String, String> getColumns();
	
}
