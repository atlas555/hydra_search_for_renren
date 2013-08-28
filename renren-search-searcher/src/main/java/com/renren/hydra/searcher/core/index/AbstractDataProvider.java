package com.renren.hydra.searcher.core.index;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;

import proj.zoie.impl.indexing.StreamDataProvider;

public abstract class AbstractDataProvider<D> extends StreamDataProvider<D> {

	protected Configuration _conf;

	protected int _partId;

	protected String _version;

	public AbstractDataProvider(Configuration conf,
			Comparator<String> versionComparator, int partId, String version) {
		super(versionComparator);
		this._conf = conf;
		this._partId = partId;
		this._version = version;
	}

	public abstract void init();

	public abstract void setStartingOffset(String version);

	public Configuration get_conf() {
		return _conf;
	}

	public void set_conf(Configuration _conf) {
		this._conf = _conf;
	}

	public int get_partId() {
		return _partId;
	}

	public void set_partId(int _partId) {
		this._partId = _partId;
	}

	public String get_version() {
		return _version;
	}

	public void set_version(String _version) {
		this._version = _version;
	}
}
