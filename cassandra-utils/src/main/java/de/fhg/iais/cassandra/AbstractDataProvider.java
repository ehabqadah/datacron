package de.fhg.iais.cassandra;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * @author kthellmann
 *
 */
public abstract class AbstractDataProvider<B> implements Serializable {

    private static final long serialVersionUID = -7715561977674121312L;

	protected ITableMetadata tableMetadata;
	protected transient final JavaSparkContext sparkContext;
	protected transient final Cassandra cassandra;
	protected final Class<B> beanClass;

    public AbstractDataProvider(//
        ITableMetadata tableMetadata, //
        JavaSparkContext sparkContext, //
        Cassandra cassandra, //
        Class<B> beanClass) {

        super();

        this.tableMetadata = tableMetadata;
        this.sparkContext = sparkContext;
        this.cassandra = cassandra;
        this.beanClass = beanClass;
        
        this.createTable();
    }

    public JavaRDD<B> createInputTableBeanRDD(B inputTableBean) {
        return this.sparkContext.parallelize(Collections.singletonList(inputTableBean));
    }

	public void updateTable(JavaRDD<B> tableBeanRDD) {
        this.cassandra.insertTableRows(tableBeanRDD, this.tableMetadata.getTableName(), this.beanClass);
    }

    public JavaRDD<B> readInputTableRows() {
        return this.cassandra.readTableRows(this.tableMetadata.getTableName(), this.beanClass);
    }
    
    public CassandraJavaPairRDD<String, B> readTableRowsByKey(String key) {
        return this.cassandra.readTableRowsByKey(this.tableMetadata.getTableName(), this.beanClass, key);
    }

    public JavaRDD<B> readInputTableRows(String arg, Object... args) {
        return this.cassandra.readTableRows(this.tableMetadata.getTableName(), this.beanClass, arg, args);
    }
    
    public JavaRDD<B> readInputTableRows(String tableName) {
        return this.cassandra.readTableRows(tableName, this.beanClass);
    }

    public <S> CassandraJavaPairRDD<S, B> joinWithTable(JavaRDD<S> tableBeanRDD, Class<S> tableClass) {
        final ImmutableMap<String, String> keyMap = this.tableMetadata.getPartitionKeys();
        final String[] keys = keyMap.keySet().toArray(new String[keyMap.size()]);

        return this.cassandra.joinTables(tableBeanRDD, this.beanClass, tableClass, this.tableMetadata.getTableName(), keys);
    }

    protected Set<String> createTableDefinition(//
        ImmutableMap<String, String> partitionKeys, //
        ImmutableMap<String, String> clusterKeys, //
        ImmutableMap<String, String> columns) {

        final Set<String> columnDefinition = Sets.union(//
            Sets.union(//
                this.cassandra.createColumnDefinition(partitionKeys), //
                this.cassandra.createColumnDefinition(clusterKeys) //
            ), //
            this.cassandra.createColumnDefinition(columns));

        final Set<String> partitionKeyDefinition = this.cassandra.createKeyDefinition(partitionKeys);
        final Set<String> clusterKeyDefinition = this.cassandra.createKeyDefinition(clusterKeys);
        final Set<String> tableDefinition = this.cassandra.createTableDefinition(//
            partitionKeyDefinition, //
            clusterKeyDefinition, //
            columnDefinition);

        return tableDefinition;
    }

    public void createTable() {
        final Set<String> tableDefinition = createTableDefinition(//
            this.tableMetadata.getPartitionKeys(), //
            this.tableMetadata.getClusterKeys(), //
            this.tableMetadata.getColumns());
		
        this.cassandra.executeCreateTableQuery(this.tableMetadata.getTableName(), tableDefinition);
    }

    public Map<String, String> getPartitionKeys() {
        return this.tableMetadata.getPartitionKeys();
    }

    public Map<String, String> getClusterKeys() {
        return this.tableMetadata.getClusterKeys();
    }

    public String getFirstPartionKey() {
        final ImmutableMap<String, String> partitionKeys = this.tableMetadata.getPartitionKeys();
        final String firstKey = Iterables.getFirst(partitionKeys.keySet(), null);
        return firstKey;
    }
	
	public String getTableName(){
		return this.tableMetadata.getTableName();
	}

	public String getRelationName(){
		return this.tableMetadata.getDescription();
	}	
	
	public ITableMetadata getTableMetadata() {
		return tableMetadata;
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public Cassandra getCassandra() {
		return cassandra;
	}

	public void close(){
		this.cassandra.close();
	}
}