package com.googlecode.jmxtrans.util;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "rrd_def")
public class RRDTemplate {
	public static class DataSource {
		public String name;
		public String type;
		public String heartbeat;
		public String min;
		public String max;
	}

	public static class Archive {
		public String cf;
		public String xff;
		public String steps;
		public String rows;
	}

	public String path;
	public int step;
	public List<DataSource> datasource;
	public List<Archive> archive;
}
