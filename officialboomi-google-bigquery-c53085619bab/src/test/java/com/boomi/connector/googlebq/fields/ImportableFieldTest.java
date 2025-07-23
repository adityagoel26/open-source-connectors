package com.boomi.connector.googlebq.fields;

import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ImportableFieldTest {

    private static final String TEST_ID = "testId";
    private static final String TEST_LABEL = "test label";
    private static final String HELP_TEXT_TEST = "help text test";
    private static final String DEFAULT_VALUE_TEST = "defValue";
    private static final ImportableField testField = new ImportableField(DataType.STRING, TEST_ID, TEST_LABEL,
            HELP_TEXT_TEST, DEFAULT_VALUE_TEST);
    private static final BrowseField field = testField.toBrowseField();

    @Test
    public void testToBrowseField() {
        assertBrowseField();
    }

    @Test
    public void testToBrowseFieldWithDefaultValue() {
        String defValue = "new test value";
        BrowseField field = testField.toBrowseField(defValue);

        assertNotNull(field.getDefaultValue());
        assertEquals(field.getDefaultValue(), defValue);
    }

    @Test
    public void testToBrowseFieldWithAllowedValues() {
        BrowseField field = testField.withAllowedValues();

        assertNotNull(field.getAllowedValues());
    }

    private void assertBrowseField() {
        assertNotNull(testField);
        assertNotNull(field);
        assertEquals(TEST_ID, field.getId());
        assertEquals(TEST_LABEL, field.getLabel());
        assertEquals(HELP_TEXT_TEST, field.getHelpText());
        assertEquals(DEFAULT_VALUE_TEST, field.getDefaultValue());
    }
}