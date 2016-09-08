package de.fhg.iais.kd.datacron.trajectories.cleaning.table.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.cassandra.ITableMetadata;

/**
 * @author kthellmann
 *
 */
public class TBMStatistics implements ITableMetadata {
	
	private static final long serialVersionUID = 9018996810732107837L;

	@Inject
	@Named("spark.app.trajectories.statistics.input")
	private String tableName;
	
	private static final String DESCRIPTION = "Trajectories statistics input.";
		
	private static final ImmutableMap<String, String> partitionKeys = ImmutableMap.<String, String> builder() //
			.put("id", "text") //
			.build();
	
	private static final ImmutableMap<String, String> clusterKeys = ImmutableMap.<String, String> builder() //
			.put("id_c", "int") //
			.build();
	
	private static final ImmutableMap<String, String> columns = ImmutableMap.<String, String> builder() //
			.put("nr_points", "int") //
			.put("min_speed", "double") //
			.put("max_speed", "double") //
			.put("avg_speed", "double") //
			.put("median_speed", "double") //
			.put("min_acceleration", "double") //
			.put("max_acceleration", "double") //
			.put("avg_acceleration", "double") //
			.put("median_acceleration", "double") //
			.put("min_difftime", "double") //
			.put("max_difftime", "double") //
			.put("avg_difftime", "double") //
			.put("median_difftime", "double") //
			.put("min_date", "double") //
			.put("max_date", "double") //
			.put("min_X", "double") //
			.put("max_X", "double") //
			.put("min_Y", "double") //
			.put("max_Y", "double") //
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
