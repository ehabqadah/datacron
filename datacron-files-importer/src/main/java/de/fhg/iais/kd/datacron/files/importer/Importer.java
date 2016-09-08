package de.fhg.iais.kd.datacron.files.importer;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.fhg.iais.kd.datacron.files.importer.data.provider.OutputProvider;
import de.fhg.iais.kd.datacron.files.importer.table.beans.TBOutput;
import scala.Tuple2;

/**
 * @author kthellmann
 *
 */
public class Importer implements Serializable {

	private static final long serialVersionUID = 6565922208286351256L;

	@Inject
	private transient JavaSparkContext sparkContext;

	@Inject
	private transient OutputProvider outputProvider;

	@Inject
	private transient Statistics dataStatistics;

	@Inject
	private Mapping mapping;

	@Inject
	@Named("spark.app.input.files")
	private String inputFiles;

	@Inject
	@Named("spark.app.trajectories.pointoutlierlist")
	private List<String> outlierPoints;

	@Inject
	@Named("spark.app.trajectories.dateoutlierlist")
	private List<String> outlierDates;

	@Inject
	@Named("spark.app.trajectories.attributeoutlierlist")
	private Map<String, List<String>> outlierAttributes;

	private Logger getLogger() {
		return Logger.getLogger(Importer.class);
	}

	public void importData() {

		// Read all input csv files from input directory and create an input RDD
		JavaPairRDD<String, String> inputRDD = sparkContext.wholeTextFiles(inputFiles, 200);

		// Create pair1RDD := ((header, line),rowCounter) from inputRDD
		JavaPairRDD<Tuple2<String, String>, Long> pair1RDD = inputRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {

					private static final long serialVersionUID = -7292933118508975020L;

					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
						final String content = tuple._2();
						final BufferedReader reader = new BufferedReader(new StringReader(content));
						final Builder<Tuple2<String, String>> lines = ImmutableList.builder();

						String header = reader.readLine();

						if (header != null) {
							String line;

							while ((line = reader.readLine()) != null) {
								lines.add(new Tuple2<>(header, line));
							}
							reader.close();

							return lines.build();
						}

						return Collections.emptyList();
					}

				}).zipWithIndex();

		// Create resultRDD = output table beans RDD without outliers
		JavaRDD<TBOutput> resultRDD = //
				pair1RDD.map(tuple -> {
					String columns = tuple._1()._1();
					String row = tuple._1()._2();
					Long index = tuple._2();

					List<String> columnNames = Arrays.asList(columns.split(","));

					List<String> ids = mapping.getId();
					int[] idColumns = new int[ids.size()];
					
					for(int i=0; i<ids.size(); i++){
						idColumns[i] = columnNames.indexOf(ids.get(i));
					}
					
					//int idColumn = columnNames.indexOf(mapping.getId());
					int dateColumn = columnNames.indexOf(mapping.getDate());
					int xColumn = columnNames.indexOf(mapping.getX());
					int yColumn = columnNames.indexOf(mapping.getY());

					Map<String, Integer> otherColumns = new LinkedHashMap<>();
					for (String othercolumn : mapping.getOther()) {
						otherColumns.put(othercolumn, columnNames.indexOf(othercolumn));
					}

					String[] rowEntries = row.split(",");
					String id = "";
					
					for(int i=0; i<idColumns.length; i++){
						int idC = idColumns[i];
						if(i > 0) {
							id += "$";
						}
						id += rowEntries[idC];
					}
					
					long id_c = index;

					String dateString = rowEntries[dateColumn];
					String xString = rowEntries[xColumn];
					String yString = rowEntries[yColumn];

					Map<String, String> additional = new LinkedHashMap<>();
					for (Map.Entry<String, Integer> entry : otherColumns.entrySet()) {
						String additionalEntryString = rowEntries[entry.getValue()];
						List<String> outliers = outlierAttributes.get(entry.getKey());

						if (outliers == null || !outliers.contains(additionalEntryString)) {
							additional.put(entry.getKey(), additionalEntryString);
						} else {
							getLogger().warn("Detected attribute outlier in trajectory " + id + "[" + id_c + "]: name="+ entry.getKey() + " outlier=" + additionalEntryString);
						}
					}

					if (!isNullOrEmpty(id) && //
					!isNullOrEmpty(xString) && //
					!isNullOrEmpty(yString) && //
					!isNullOrEmpty(dateString) && //
					!outlierPoints.contains(xString) && //
					!outlierPoints.contains(yString) && //
					!outlierDates.contains(dateString)) {
						return new TBOutput(id, id_c, dateString, xString, yString, additional);
					} else {
						getLogger().warn("Detected point/date outlier in trajectory " + id + "[" + id_c + "]: x=" + xString+ " y=" + yString + " date=" + dateString);
					}

					return null;
				}).filter(record -> record != null);
		
		// Cache resultRDD
		resultRDD.persist(StorageLevel.MEMORY_ONLY());

		// Compute input data statistics
		dataStatistics.generate(resultRDD, outputProvider.getTableName());
		
		// Write data to table
		this.outputProvider.updateTable(resultRDD);	

		// Close connection to Cassandra store
		this.outputProvider.close();
	}

}
