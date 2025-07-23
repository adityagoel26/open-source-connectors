// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.batch;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TempOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
@Ignore
@RunWith(Parameterized.class)
public class BatchFactoryMultipleSuffixTest {
    private static final String DEFAULT_SUFFIX = StringUtil.EMPTY_STRING;
    private static final int MAX_BYTES = 1000000;
    private static final String CONTENT =
            "{\"ADDITIONAL_BONUS\": 2900,\"ANNUAL_BONUS\": 20000,\"BADGE_NUMBER\": 15650,\"BONUS_ELIGIBLE\": "
                    + "\"TRUE\",\"CAREER_ID\": \"I7\",\"EMPLOYEE_ID\": \"38473874AF\",\"MANAGER_ID\": \"102\","
                    + "\"OFFICE_NUMBER\": 95,\"SALARY\": 115000}";
    private final OperationResponse _response = mock(OperationResponse.class);

    private final int _documents;
    private final int _suffix;
    private final int _batchCount;

    private TempOutputStream.TempInputStream _content;
    private long _start;
    private int _count = 0;
    private int _largest = 0;

    public BatchFactoryMultipleSuffixTest(int documents, int suffix, int batchCount) {
        _documents = documents;
        _suffix = suffix;
        _batchCount = batchCount;
    }

    @Parameterized.Parameters(name = "{0}: {1}, {2}")
    public static Collection<Object> getParameters() {
        Collection<Object> params = new ArrayList<>();
        params.add(new Object[] { 10000000, 1000, 100 });
        params.add(new Object[] { 10000000, 100, 1000 });
        params.add(new Object[] { 10000000, 1000, 1000 });
        params.add(new Object[] { 10000000, 100000, 1 });
        params.add(new Object[] { 10000000, 10, 10000 });
        return params;
    }

    @Before
    public void setup() {
        _content = read(CONTENT);
        _start = System.currentTimeMillis();
    }

    @After
    public void tearDown() {
        String runtime = String.valueOf(System.currentTimeMillis() -_start);
        System.out.print("Docs:" + _documents + ", Suffix:" + _suffix + ", BatchSize:" + _batchCount);
        System.out.println(" Count:" + _count + ", Largest:" + _largest + ", Time:" + runtime);
        IOUtil.closeQuietly(_content);
    }

    @Test
    public void testMultipleTemplatesuffix() throws Exception {

        UpdateRequest updateRequest = createUpdateRequest(_content, _documents, _suffix);
        BatchFactory factory = new BatchFactory(updateRequest, DEFAULT_SUFFIX, _batchCount, MAX_BYTES, _response);
        for (Batch batch : factory) {
            InputStream inputStream = null;
            for (ObjectData document : batch) {
                try {
                    inputStream = document.getData();
                }
                finally {
                    IOUtil.closeQuietly(inputStream);
                }
            }
            //TimeUnit.MILLISECONDS.sleep(200);
            _count++;
            _largest = _largest > batch.getBatchCount() ? _largest : batch.getBatchCount();
        }
    }

    private static TempOutputStream.TempInputStream read(String str) {
        TempOutputStream bos = null;
        try {
            bos = new TempOutputStream();
            bos.write(str.getBytes(Charset.defaultCharset()));
            return (TempOutputStream.TempInputStream) bos.toInputStream();
        }
        catch (IOException e) {
            throw new ConnectorException(e);
        }
        finally {
            IOUtil.closeQuietly(bos);
        }
    }

    private UpdateRequest createUpdateRequest(final TempOutputStream.TempInputStream data, final int amount,
            final int suffix) {
        return new UpdateRequest() {

            int _count = 0;
            final Map<String, String> dynProps = new HashMap<>();
            final Map<String, String> userProps = new HashMap<>();

            @Override
            public Iterator<ObjectData> iterator() {
                return new Iterator<ObjectData>() {

                    @Override
                    public boolean hasNext() {
                        return _count < amount;
                    }

                    @Override
                    public ObjectData next() {
                        String templateSuffix = String.valueOf(new Random().nextInt(suffix));
                        dynProps.put("templateSuffix", templateSuffix);
                        return new SimpleTrackedData(_count++, data.share(), userProps, dynProps);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
