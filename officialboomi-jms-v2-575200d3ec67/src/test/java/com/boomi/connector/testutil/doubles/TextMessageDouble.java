// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil.doubles;

import com.boomi.util.StringUtil;

import javax.jms.TextMessage;

public class TextMessageDouble extends MessageDouble implements TextMessage {

    private String _value;

    public TextMessageDouble(String destination, String value) {
        super(destination);
        _value = value;
    }

    public TextMessageDouble() {
        this(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING);
    }

    @Override
    public void setText(String string) {
        _value = string;
    }

    @Override
    public String getText() {
        return _value;
    }
}
