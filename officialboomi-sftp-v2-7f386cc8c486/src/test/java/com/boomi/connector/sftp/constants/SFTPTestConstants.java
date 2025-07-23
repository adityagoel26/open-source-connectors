// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp.constants;

/**
 * @author mukulrana.
 */
public class SFTPTestConstants {
    private SFTPTestConstants() {
        //Hide implicit constructor
    }
    /** The Constant FILE1_TXT. */
    public static final String FILE1_TXT = "file1.txt";

    /** The Constant MOCK_FILE_NAME. */
    public static final String MOCK_FILE_NAME = "yweurywi";

    /** The Constant ERROR_OCCURRED. */
    public static final String ERROR_OCCURRED = "Error occurred";

    /** The Constant OVERWRITE. */
    public static final String OVERWRITE = "OVERWRITE";

    /** The Constant FILE_EXISTS. */
    public static final String FILE_EXISTS = "File Exists";

    /** The Constant TEST_GET_CLIENT. */
    public static final String TEST_GET_CLIENT = "testGetClient";
    /** The Constant TEST_RENAME_FILE. */
    public static final String TEST_RENAME_FILE = "testRenameFile";
    /** The Constant TEST_GET_CLIENT_USER_NAME_PASSWORD. */
    public static final String TEST_GET_CLIENT_USER_NAME_PASSWORD = "testGetClientUserNamePassword";
    /** The Constant TEST_GET_PATHS_HANDLER. */
    public static final String TEST_GET_PATHS_HANDLER = "testGetPathsHandler";
    /** The Constant TEST_IS_CONNECTED. */
    public static final String TEST_IS_CONNECTED = "testIsConnected";
    /** The Constant TEST_CLOSE_CONNECTION. */
    public static final String TEST_CLOSE_CONNECTION = "testCloseConnection";

    public static final String TEST_EXECUTE_DELETE_DELETE_REQUEST_OPERATION_RESPONSE =
            "testExecuteDeleteDeleteRequestOperationResponse";
    public static final String TEST_EXECUTE_DELETE_DELETE_REQUEST_OPERATION_RESPONSE_DELETE_FAILED =
            "testExecuteDeleteDeleteRequestOperationResponseDeleteFailed";
    public static final String TEST_EXECUTE_DELETE_DELETE_REQUEST_OPERATION_RESPONSE_DELETE_FAILED_EXCEPTION =
            "testExecuteDeleteDeleteRequestOperationResponseDeleteFailedException";
    public static final String TEST_EXECUTE_DELETE_DELETE_REQUEST_OPERATION_RESPONSE_DELETE_FAILED_OPEN_CONNECTION =
            "testExecuteDeleteDeleteRequestOperationResponseDeleteFailedOpenConnection";
    public static final String TEST_SFTP_GET_OPERATION = "testSFTPGetOperation";
    public static final String TEST_SFTP_GET_OPERATION_DELETE_DISABLED = "testSFTPGetOperationDeleteDisabled";
    public static final String TEST_SFTP_GET_OPERATION_FAIL_AFTER_DELETE = "testSFTPGetOperationFailAfterDelete";
    public static final String TEST_SFTP_GET_OPERATION_DELETE_FAILED = "testSFTPGetOperationDeleteFailed";
    public static final String TEST_SFTP_GET_OPERATION_BLANK_FILE_N_AME = "testSFTPGetOperationBlankFileNAme";
    public static final String TEST_SFTP_GET_OPERATION_FILE_NOT_FOUND = "testSFTPGetOperationFileNotFound";
}
