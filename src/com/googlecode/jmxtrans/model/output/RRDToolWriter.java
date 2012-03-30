package com.googlecode.jmxtrans.model.output;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.JmxUtils;
import com.googlecode.jmxtrans.util.RRDTemplate;
import com.googlecode.jmxtrans.util.ValidationException;

/**
 * This takes a JRobin template.xml file and then creates the database if it
 * doesn't already exist.
 * 
 * It will then write the contents of the Query (the Results) to the database.
 * 
 * This method exec's out to use the command line version of rrdtool. You need
 * to specify the path to the directory where the binary rrdtool lives.
 * 
 * @author jon
 */
public class RRDToolWriter extends BaseOutputWriter {
	private static final Logger log = LoggerFactory.getLogger(RRDToolWriter.class);

	private File outputFile = null;
	private File templateFile = null;
	private File binaryPath = null;
	public static final String GENERATE = "generate";
	private static final char[] INITIALS = { ' ', '.' };

	/** */
	public RRDToolWriter() {
	}

	public void validateSetup(Query query) throws ValidationException {
		outputFile = new File((String) this.getSettings().get(OUTPUT_FILE));
		templateFile = new File((String) this.getSettings().get(TEMPLATE_FILE));
		binaryPath = new File((String) this.getSettings().get(BINARY_PATH));

		if (outputFile == null || templateFile == null || binaryPath == null) {
			throw new ValidationException("output, template and binary path file can't be null", query);
		}
	}

	/**
	 * rrd datasources must be less than 21 characters in length, so work to
	 * make it shorter. Not ideal at all, but works fairly well it seems.
	 */
	public String getDataSourceName(String typeName, String attributeName, String entry) {

		String result = null;
		if (typeName != null) {
			result = typeName + attributeName + entry;
		} else {
			result = attributeName + entry;
		}

		if (attributeName.length() > 15) {
			String[] split = StringUtils.splitByCharacterTypeCamelCase(attributeName);
			String join = StringUtils.join(split, '.');
			attributeName = WordUtils.initials(join, INITIALS);
		}
		result = attributeName + DigestUtils.md5Hex(result);

		result = StringUtils.left(result, 19);

		return result;
	}

	/** */
	public void doWrite(Query query) throws Exception {
		RRDTemplate def = getDatabaseTemplateSpec();

		List<String> dsNames = getDsNames(def);
		List<Result> results = query.getResults();

		Map<String, String> dataMap = new TreeMap<String, String>();

		// go over all the results and look for datasource names that map to
		// keys from the result values
		for (Result res : results) {
			log.debug(res.toString());
			Map<String, Object> values = res.getValues();
			if (values != null) {
				for (Entry<String, Object> entry : values.entrySet()) {
					String key = getDataSourceName(getConcatedTypeNameValues(res.getTypeName()), res.getAttributeName(), entry.getKey());
					boolean isNumeric = JmxUtils.isNumeric(entry.getValue());

					if (isDebugEnabled() && isNumeric) {
						log.debug("Generated DataSource name:value: " + key + " : " + entry.getValue());
					}

					if (dsNames.contains(key) && isNumeric) {
						dataMap.put(key, entry.getValue().toString());
					}
				}
			}
		}

		doGenerate(results);

		if (dataMap.keySet().size() > 0 && dataMap.values().size() > 0) {
			rrdToolUpdate(StringUtils.join(dataMap.keySet(), ':'), StringUtils.join(dataMap.values(), ':'));
		} else {
			log.error("Nothing was logged for query: " + query);
		}
	}

	private void doGenerate(List<Result> results) throws Exception {
		if (isDebugEnabled() && this.getBooleanSetting(GENERATE)) {
			StringBuilder sb = new StringBuilder("\n");
			List<String> keys = new ArrayList<String>();

			for (Result res : results) {
				Map<String, Object> values = res.getValues();
				if (values != null) {
					for (Entry<String, Object> entry : values.entrySet()) {
						boolean isNumeric = JmxUtils.isNumeric(entry.getValue());
						if (isNumeric) {
							String key = getDataSourceName(getConcatedTypeNameValues(res.getTypeName()), res.getAttributeName(), entry.getKey());
							if (keys.contains(key)) {
								throw new Exception("Duplicate datasource name found: '" + key
										+ "'. Please try to add more typeName keys to the writer to make the name more unique. " + res.toString());
							}
							keys.add(key);

							sb.append("<datasource><!-- " + res.getTypeName() + ":" + res.getAttributeName() + ":" + entry.getKey() + " --><name>"
									+ key + "</name><type>GAUGE</type><heartbeat>400</heartbeat><min>U</min><max>U</max></datasource>\n");
						}
					}
				}
			}
			log.debug(sb.toString());
		}
	}

	/**
	 * Executes the rrdtool update command.
	 */
	protected void rrdToolUpdate(String template, String data) throws Exception {
		List<String> commands = new ArrayList<String>();
		commands.add(binaryPath + "/rrdtool");
		commands.add("update");
		commands.add(outputFile.getCanonicalPath());
		commands.add("-t");
		commands.add(template);
		commands.add("N:" + data);

		ProcessBuilder pb = new ProcessBuilder(commands);
		Process process = pb.start();
		try {
            checkErrorStream(process);
        }
        finally {
            IOUtils.closeQuietly(process.getInputStream());
            IOUtils.closeQuietly(process.getOutputStream());
            IOUtils.closeQuietly(process.getErrorStream());
        }
	}

	/**
	 * If the database file doesn't exist, it'll get created, otherwise, it'll
	 * be returned in r/w mode.
	 */
	protected RRDTemplate getDatabaseTemplateSpec() throws Exception {
		Unmarshaller u = JAXBContext.newInstance(RRDTemplate.class).createUnmarshaller();
		RRDTemplate t = (RRDTemplate) u.unmarshal(templateFile);

		if (!this.outputFile.exists()) {
			FileUtils.forceMkdir(this.outputFile.getParentFile());
			rrdToolCreateDatabase(t);
		}
		return t;
	}

	/**
	 * Calls out to the rrdtool binary with the 'create' command.
	 */
	protected void rrdToolCreateDatabase(RRDTemplate t) throws Exception {
		List<String> commands = new ArrayList<String>();
		commands.add(this.binaryPath + "/rrdtool");
		commands.add("create");
		commands.add(this.outputFile.getCanonicalPath());
		commands.add("-s");
		commands.add(String.valueOf(t.step));

		for (RRDTemplate.DataSource dsdef : t.datasource) {
			commands.add(String.format("DS:%s:%s:%s:%s:%s", dsdef.name, dsdef.type, dsdef.heartbeat, dsdef.min, dsdef.max));
		}

		for (RRDTemplate.Archive archive : t.archive) {
			commands.add(String.format("RRA:%s:%s:%s:%s", archive.cf, archive.xff, archive.steps, archive.rows));
		}
		ProcessBuilder pb = new ProcessBuilder(commands);
		Process process = pb.start();
		try {
			checkErrorStream(process);
		} finally {
			IOUtils.closeQuietly(process.getInputStream());
			IOUtils.closeQuietly(process.getOutputStream());
			IOUtils.closeQuietly(process.getErrorStream());
		}
	}

	/**
	 * Check to see if there was an error processing an rrdtool command
	 */
	private void checkErrorStream(Process process) throws Exception {
		InputStream is = process.getErrorStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		StringBuilder sb = new StringBuilder();
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		if (sb.length() > 0) {
			throw new RuntimeException(sb.toString());
		}
	}

	/**
	 * Get a list of DsNames used to create the datasource.
	 */
	private List<String> getDsNames(RRDTemplate def) {
		List<String> names = new ArrayList<String>();
		for (RRDTemplate.DataSource ds : def.datasource) {
			names.add(ds.name);
		}
		return names;
	}
}
