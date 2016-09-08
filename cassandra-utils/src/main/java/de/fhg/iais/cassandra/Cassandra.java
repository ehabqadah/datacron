package de.fhg.iais.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.allColumns;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import java.io.Serializable;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;


/**
 * @author kthellmann
 *
 */
@Singleton
public class Cassandra implements Serializable {

	private static final long serialVersionUID = 5077443248183372148L;
	
	private final String keyspace;
	private final String replicationFactor;
	private final String replicationStrategy;
	
	private transient final Cluster cluster;
	private transient final Session session;
	private transient final JavaSparkContext sparkContext;
	
	@Inject
	public Cassandra(//
			@Named("cassandra.seed") String seed, //
			@Named("cassandra.keyspace") String keyspace, //
			@Named("cassandra.replication.factor") String replicationFactor, //
			@Named("cassandra.replication.strategy") String replicationStrategy,//
			JavaSparkContext sparkContext) {

		this.keyspace = keyspace;
		this.replicationFactor = replicationFactor;
		this.replicationStrategy = replicationStrategy;

		this.cluster = Cluster.builder().addContactPoint(seed).build();
		this.session = this.cluster.connect();
		
		this.sparkContext = sparkContext;
		
		this.executeKeyspaceQuery();
		
	}

	public void executeKeyspaceQuery() {
		StringBuilder keyspaceQuery = new StringBuilder();

		keyspaceQuery.append("CREATE KEYSPACE IF NOT EXISTS ");
		keyspaceQuery.append(this.keyspace);
		keyspaceQuery.append(" WITH REPLICATION = { 'class':'" + this.replicationStrategy + "', 'replication_factor':"+ this.replicationFactor + " };");

		this.session.execute(keyspaceQuery.toString());
	}

	public void executeCreateTableQuery(String tableName, Set<String> tableDefinition) {
		StringBuilder tableQuery = new StringBuilder();

		tableQuery.append("CREATE TABLE IF NOT EXISTS ");
		tableQuery.append(this.keyspace);
		tableQuery.append(".");
		tableQuery.append(tableName);
		tableQuery.append("(");
		Joiner.on(", ").appendTo(tableQuery, tableDefinition);
		tableQuery.append("); ");
		
		this.session.execute(tableQuery.toString());
	}

	public void executeCopyWithHeaderQuery(String tableName, Set<String> tableDefinition, String pathToCSV) {
		StringBuilder copyQuery = new StringBuilder();

		copyQuery.append("COPY ");
		copyQuery.append(this.keyspace);
		copyQuery.append(".");
		copyQuery.append(tableName);
		copyQuery.append("(");
		Joiner.on(", ").appendTo(copyQuery, tableDefinition);
		copyQuery.append(") ");
		copyQuery.append("TO ");
		copyQuery.append("'" + pathToCSV + "'");
		copyQuery.append(" ;");

		this.session.execute(copyQuery.toString());
	}

	public Set<String> createColumnDefinition(ImmutableMap<String, String> columns) {
		ImmutableSet.Builder<String> columnBuilder = ImmutableSet.builder();

		for (String key : columns.keySet()) {
			String type = columns.get(key);
			columnBuilder.add(new StringBuilder().append(key).append(" ").append(type).toString());
		}

		return columnBuilder.build();
	}

	public Set<String> createKeyDefinition(ImmutableMap<String, String> keys) {
		ImmutableSet.Builder<String> keyBuilder = ImmutableSet.builder();

		for (String partitionkey : keys.keySet()) {
			keyBuilder.add(new StringBuilder().append(partitionkey).append(" ").toString());
		}

		return keyBuilder.build();
	}

	public Set<String> createTableDefinition(Set<String> partitionKeyDefinition, Set<String> clusterKeyDefinition, Set<String> columnDefinition) {
		ImmutableSet.Builder<String> keyBuilder = ImmutableSet.builder();

		StringBuilder partitionKeyBuilder = new StringBuilder();
		partitionKeyBuilder.append(" (");
		Joiner.on(", ").appendTo(partitionKeyBuilder, partitionKeyDefinition);
		partitionKeyBuilder.append(") ");

		StringBuilder compoundKeyBuilder = new StringBuilder();
		compoundKeyBuilder.append("PRIMARY KEY");
		compoundKeyBuilder.append(" (");
		compoundKeyBuilder.append(partitionKeyBuilder.toString());
		compoundKeyBuilder.append(!clusterKeyDefinition.isEmpty()? ", ":" ");
		Joiner.on(", ").appendTo(compoundKeyBuilder, clusterKeyDefinition);
		compoundKeyBuilder.append(") ");

		keyBuilder.add(compoundKeyBuilder.toString());

		return Sets.union(keyBuilder.build(), columnDefinition);
	}

	public <T> void insertTableRows(JavaRDD<T> tableObjectRDD, String tableName, Class<T> tableClass) {
		javaFunctions(tableObjectRDD).writerBuilder(this.keyspace, tableName, mapToRow(tableClass)).saveToCassandra();
	}

	public <T> CassandraTableScanJavaRDD<T> readTableRows(String tableName, Class<T> tableClass) {
		return javaFunctions(sparkContext).cassandraTable(this.keyspace, tableName, mapRowTo(tableClass));
	}
	
	public <T, S> CassandraJavaPairRDD<S, T> joinTables(JavaRDD<S> tableObjectRDD, Class<T> tableClass1, Class<S> tableClass2, String tableName, String[] args) {
		ColumnSelector selectedColumns = allColumns;
		ColumnSelector joinColumns = someColumns(args);
		RowReaderFactory<T> rowReaderFactory =  mapRowTo(tableClass1);
		RowWriterFactory<S> rowWriterFactory = mapToRow(tableClass2);
		
		return javaFunctions(tableObjectRDD).joinWithCassandraTable(this.keyspace, tableName, selectedColumns, joinColumns, rowReaderFactory, rowWriterFactory);
	}

	public <T> CassandraTableScanJavaRDD<T> readTableRows(String tableName, Class<T> tableClass, String arg, Object... args) {
		return javaFunctions(sparkContext).cassandraTable(this.keyspace, tableName, mapRowTo(tableClass)).where(arg, args);
	}
	
	public <T> CassandraJavaPairRDD<String, T> readTableRowsByKey(String tableName, Class<T> tableClass, String key){
		return javaFunctions(sparkContext).cassandraTable(this.keyspace, tableName, mapRowTo(tableClass)).keyBy(mapColumnTo(String.class), String.class, key);
	}
	
	public <K, T> CassandraJavaPairRDD<K, T> readTableRowsByKey(String tableName, Class<T> tableClass, RowReaderFactory<K> keyRowReaderFactory,  String... keys){
		Class<K> keyClass = keyRowReaderFactory.targetClass();
		return javaFunctions(sparkContext).cassandraTable(this.keyspace, tableName, mapRowTo(tableClass)).keyBy(keyRowReaderFactory, keyClass, keys);
	}

	public void close() {
		this.session.close();
		this.cluster.close();
	}

}
