package org.wikimedia.eventutilities.flink.test.utils;

import java.util.concurrent.Callable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

public final class FlinkTestUtils {

    /**
     * Get a MiniClusterWithClientResource to use in tests.
     */
    public static MiniClusterWithClientResource getTestFlinkCluster() {
        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build()
        );
    }

    /**
     * Register a callback to be called after successful completion of a FLink job in the
     * StreamExecutionEnvironment. Throws a RuntimeException if any part of the Flink job fails.
     * Useful for running test assertions after job results are collected in a Sink.
     *
     * @param env StreamExecutionEnvironment
     * @param callback 0 argument callback to call on successful job completion.
     */
    public static void afterFlinkJob(StreamExecutionEnvironment env, Callable<Object> callback) {
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(JobClient jobClient, Throwable throwable)  {
                // We just raise any throwable, otherwise no-op.
                if (throwable != null) {
                    throw new RuntimeException(throwable);
                }
            }

            //Callback on job execution finished, successfully or unsuccessfully.
            @SuppressWarnings("checkstyle:IllegalCatch")
            @Override
            public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable)  {
                // Raise any throwable, else call the callback
                if (throwable != null) {
                    throw new RuntimeException(throwable);
                } else {
                    try {
                        callback.call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /**
     * Constructor to make sure this class is never instantiated by mistake.
     * See: https://checkstyle.sourceforge.io/config_design.html#HideUtilityClassConstructor
     */
    private FlinkTestUtils() {}

}
