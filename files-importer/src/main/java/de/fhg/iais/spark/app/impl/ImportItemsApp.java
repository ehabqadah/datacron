package de.fhg.iais.spark.app.impl;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.generated.table.beans.TBItems;
import de.fhg.iais.generated.table.columns.metadata.TBMItems;
import de.fhg.iais.spark.cassandra.Cassandra;
import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
public class ImportItemsApp implements ISparkApp {
	private static final long serialVersionUID = -6003951050172782681L;

	private final String inputFilePath;
	private final TBMItems tbmItems;
	private transient final Cassandra cassandra;
	private transient final JavaSparkContext sparkContext;

	@Inject
	public ImportItemsApp(//
			@Named("spark.importer.data") String inputFilePath, //
			JavaSparkContext sparkContext, //
			Cassandra cassandra, //
			TBMItems tbmItems) {

		this.cassandra = cassandra;
		this.sparkContext = sparkContext;
		this.inputFilePath = inputFilePath;

		this.cassandra.executeKeyspaceQuery();

		this.tbmItems = tbmItems;

		ImmutableMap<String, String> columns = tbmItems.getColumns();
		Set<String> columnDefinition = this.cassandra.createColumnDefinition(columns);

		ImmutableMap<String, String> partitionKeys = tbmItems.getPartitionKeys();
		Set<String> partitionKeyDefinition = this.cassandra.createKeyDefinition(partitionKeys);

		ImmutableMap<String, String> clusterKeys = tbmItems.getClusterKeys();
		Set<String> clusterKeyDefinition = this.cassandra.createKeyDefinition(clusterKeys);

		Set<String> tableDefinition = this.cassandra.createTableDefinition(partitionKeyDefinition, clusterKeyDefinition, columnDefinition);

		this.cassandra.executeCreateTableQuery(tbmItems.getTableName(), tableDefinition);
	}

	public void run() {
		JavaPairRDD<String, String> inputRDD = sparkContext.wholeTextFiles(inputFilePath, 200);

		JavaRDD<TBItems> itemsBean = //
				inputRDD.map(new Function<Tuple2<String, String>, TBItems>() {
					private static final long serialVersionUID = 4920857731378485230L;

					@Override
					public TBItems call(Tuple2<String, String> tuple) throws Exception {
						String rdfdump = tuple._2();
						UUID id = UUID.randomUUID();
						ByteBuffer content = ByteBuffer.wrap(rdfdump.getBytes("UTF-8"));					
						DateTime ingestdate = DateTime.now();
																		
						return new TBItems(//
								id.toString(), //
								content, //							
								ingestdate);
					}

				});

		this.cassandra.insertTableRows(itemsBean, tbmItems.getTableName(), TBItems.class);

		this.cassandra.close();
	}

}