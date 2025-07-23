// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import java.io.IOException;
import java.io.InputStream;

public class XMLStreamOmittingDeclaration extends InputStream {
    private final InputStream _inputData;
    private boolean _isFirst;
    private boolean _isSecond;

    public XMLStreamOmittingDeclaration(InputStream inputData) {
        _inputData = inputData;
        _isFirst = true;
        _isSecond = false;
    }

    @Override
    public int read() throws IOException {
        if (_isFirst) {
            int curChar;
            do {
                curChar = _inputData.read();

            } while (curChar != -1 && curChar != (int) '<');

            _isFirst = false;
            _isSecond = true;
            return curChar;
        } else if (_isSecond) {
            int curChar = _inputData.read();
            if (curChar == (int) '?') {
                // contains XML declaration
                return skipXMLDeclaration();
            }
            return curChar;
        } else {
            return _inputData.read();
        }
    }

    private int skipXMLDeclaration() throws IOException {
        int curChar;
        do {
            curChar = _inputData.read();
        } while (curChar != -1 && curChar != (int) '<');

        if (curChar == -1) {
            // reached end of file before root element
            return -1;
        }
        return _inputData.read();
    }

    @Override
    public void close() throws IOException {
        super.close();
        _inputData.close();
    }
}
