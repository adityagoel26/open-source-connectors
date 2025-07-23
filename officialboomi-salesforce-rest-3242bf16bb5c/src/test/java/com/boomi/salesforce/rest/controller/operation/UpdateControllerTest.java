// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.controller.operation;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class UpdateControllerTest {

    private static final byte[] INPUT_PAYLOAD = ("<records type=\"Account\" url=\"/services/data/v50"
                                                 + ".0/sobjects/Account/0014K00000Bq6wFQAR\"><Id>0014K00000HP33vQAD"
                                                 + "</Id><Name>sobjects " + "name</Name></records>").getBytes(
            StringUtil.UTF8_CHARSET);

    @Test
    public void getRecordID() {
        SFRestConnection connection = Mockito.mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);
        InputStream inputData = new ByteArrayInputStream(INPUT_PAYLOAD);

        UpdateController controller = new UpdateController(connection, "sobject", inputData);
        String recordId = controller.getRecordId();

        Assertions.assertEquals("0014K00000HP33vQAD", recordId);
    }
}
