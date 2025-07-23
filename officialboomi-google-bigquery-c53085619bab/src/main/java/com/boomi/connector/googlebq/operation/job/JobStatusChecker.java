//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.connector.googlebq.resource.JobResource;

import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Auxiliary class to verify the status from a {@link Job}
 */
public class JobStatusChecker {

    private JobStatusChecker() {

    }

    /**
     * Checks the status by calling {@link JobResource#getJob(String, String)} to get the updated version of the job
     * resource. The updated version of job resource will contain the latest state of the job. This method will continue
     * to call {@link JobResource#getJob(String, String)} until the job state is DONE and {@link Job#isJobDone()} is
     * true or a configured timeout occurs in which case {@link TimeoutRetry#shouldRetry(int, Object)} returns false. If
     * the call to  {@link JobResource#getJob(String, String)} fails, the error response will be added by on {@link
     * Job#updateJobResponse(Response)}.
     * <p>
     *
     * @param job
     */
    public static Job checkJobStatus(Job job, TimeoutRetry strategy, JobResource jobResource)
            throws GeneralSecurityException, IOException {
        int numAttempts = 0;

        Job newJob;
        do {
            Response response = jobResource.getJob(job.getJobId(), job.getLocation());
            newJob = new Job(response);
            if (newJob.isError()) {
                return newJob;
            }
            numAttempts++;
        } while (!newJob.isJobDone() && continueStatusCheck(strategy, numAttempts));
        return newJob;
    }

    /**
     * Determines if we should continue checking the status of the submitted job. {@link
     * TimeoutRetry#shouldRetryImpl(int, Object)} checks if the elapsed time since the first status check request has
     * exceeded the timeout set by user.
     *
     * If the timeout has occurred the method returns false. The input document is marked {@link
     * OperationStatus#APPLICATION_ERROR} in this case.
     *
     * If timeout has not passed {@link com.boomi.util.retry.SleepingRetry#backoff(int)} is called which puts the
     * current thread to sleep for a duration depending on the number of attempt.
     *
     * If the timeout is configured as -1 then {@link TimeoutRetry#shouldRetryImpl(int, Object)} returns true and retry
     * is performed indefinitely
     *
     * @param strategy
     * @param numAttempts
     * @return
     */
    private static boolean continueStatusCheck(TimeoutRetry strategy, int numAttempts) {
        if (strategy.shouldRetry(numAttempts, null)) {
            strategy.backoff(numAttempts);
            return true;
        } else {
            return false;
        }
    }
}
