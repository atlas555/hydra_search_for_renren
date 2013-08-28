package com.renren.hydra.indexing.provider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.renren.hydra.searcher.core.index.AbstractDataProvider;

import proj.zoie.api.DataConsumer.DataEvent;

public abstract class LinedFileDataProvider<D> extends AbstractDataProvider<D> {

	private static final Logger logger = Logger
			.getLogger(LinedFileDataProvider.class);

	private final File _file;
	private long _offset;
	private boolean _stop;

	private BufferedReader _rad;

	public LinedFileDataProvider(Configuration conf,
			Comparator<String> versionComparator, int partId, String version) {
		super(conf, versionComparator, partId, version);
		
		//_offset = version == null ? 0L : Long.parseLong(version);
		
		_file = new File(conf.getString("path"));
		if (_file.isFile() && _file.exists()) {
			try {
				_rad = new BufferedReader(new FileReader(_file));
			} catch (Exception e) {
				_rad = null;
			}
		} else {
			_rad = null;
		}
		_stop = false;
		_offset = 0;
	}

	protected abstract D convertLine(String line) throws IOException;

	@Override
	public DataEvent<D> next() {
		DataEvent<D> event = null;
		if (!_stop && _rad != null) {
			try {
				String line = _rad.readLine();
				if (line == null)
					return null;
				D dataObj = convertLine(line);
				String version = "0:0";
				event = new DataEvent<D>(dataObj, version);
			} catch (IOException ioe) {
				logger.error(ioe.getMessage(), ioe);
			}
		}
		return event;
	}

	@Override
	public void setStartingOffset(String version) {
	}

	@Override
	public void reset() {
		try {
			if (_rad != null) {
				_rad.close();
				_rad = new BufferedReader(new FileReader(_file));
				_offset = 0;
				_stop = false;
			}
		} catch (IOException ioe) {
			logger.error(ioe.getMessage(), ioe);
		}
	}

	@Override
	public void start() {
		super.start();
		_stop = false;
	}

	@Override
	public void stop() {
		_stop = true;
	}

}

