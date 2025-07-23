// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import java.io.IOException;
import java.io.InputStream;

import com.boomi.util.IOUtil;

public class JsonArrayStream extends InputStream {

	private InputStream _stream;
	private boolean _firstCall;
	private boolean _isDone;
	private int _nxtChar;

	public JsonArrayStream(InputStream stream) {
		_stream = stream;
		_firstCall = true;
		_isDone = false;
	}

	@Override
	public int read() throws IOException {
		if (_firstCall) {
			_firstCall = false;
			_nxtChar = _stream.read();
			return Integer.valueOf('[');
		}
		if (_isDone) {
			return -1;
		}
		int curChar = _nxtChar;
		_nxtChar = _stream.read();
		if (curChar == Integer.valueOf('\n') && _nxtChar != -1) {
			return Integer.valueOf(',');
		}
		if (curChar == -1) {
			_isDone = true;
			return Integer.valueOf(']');
		}
		return curChar;
	}

	@Override
	public void close() {
		IOUtil.closeQuietly(_stream);
	}
}
