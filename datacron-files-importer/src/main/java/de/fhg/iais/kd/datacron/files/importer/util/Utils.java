package de.fhg.iais.kd.datacron.files.importer.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class Utils implements Serializable {

	private static final long serialVersionUID = 1072219813274243606L;

	@Inject
	@Named("spark.app.trajectories.statistics")
	private String statisticsOutputDir;

	public void appendToFile(String filename, String... lines) {
		try {

			final File file = new File(statisticsOutputDir + "/" + filename);
			if (!file.exists()) {
				file.createNewFile();
			}

			final FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			final BufferedWriter bw = new BufferedWriter(fw);

			Joiner.on("\n").appendTo(bw, lines);
			bw.append("\n");
			bw.close();

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
