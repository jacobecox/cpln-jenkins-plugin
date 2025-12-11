package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.AsyncPeriodicWork;
import hudson.model.Executor;
import hudson.model.Node;
import hudson.model.Queue;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.remoting.Channel;
import hudson.remoting.VirtualChannel;
import jenkins.model.Jenkins;
import hudson.node_monitors.ResponseTimeMonitor;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * StalledBuildDetector detects builds that have completed in the agent but whose
 * completion event was never received by Jenkins due to remoting stalls.
 * 
 * This addresses a SPECIFIC failure mode where:
 * - The build script finishes successfully on the agent (logs show completion)
 * - Jenkins never receives the final RPC indicating build step completion
 * - The agent remains online but with very high response time (5-10 seconds)
 * - No new stdout arrives after the build completes
 * - The Jenkins executor remains stuck showing the build still running
 * - Standard cleanup never happens because no events fire
 * 
 * This is DISTINCT from:
 * - Early exit (agent crashes) - handled by CplnCleanupListener
 * - JVM freeze (agent totally unresponsive) - handled by StuckAgentDetector
 * 
 * DETECTION CRITERIA (ALL must be true):
 * 1. Agent is online
 * 2. Executor is busy (Jenkins thinks build is still running)
 * 3. No log output for > LOG_QUIET_PERIOD (e.g., 60 seconds)
 * 4. Channel shows signs of staleness (no recent activity)
 * 5. Workload is still Running/Ready in CPLN (not crashed/restarting)
 * 6. This state has persisted for > STALLED_GRACE_PERIOD
 * 
 * FALSE POSITIVE PREVENTION:
 * - Requires log quiet period (build must have stopped producing output)
 * - Requires channel staleness (not just slow builds)
 * - Requires multiple detection cycles
 * - Long grace period before action
 * - Never affects builds still producing output
 * 
 * IMPORTANT: This detector operates on BUSY executors, unlike StuckAgentDetector
 * which operates on IDLE executors.
 */
@Extension
@SuppressFBWarnings
public class StalledBuildDetector extends AsyncPeriodicWork {

    private static final Logger LOGGER = Logger.getLogger(StalledBuildDetector.class.getName());

    // Check interval in milliseconds (default: 20 seconds)
    private static final long CHECK_INTERVAL_MS = Long.parseLong(
            System.getProperty("cpln.stalled.check.interval.ms", "20000"));

    // Log quiet period - no output for this long suggests build finished (default: 60 seconds)
    // This must be long enough to avoid false positives on builds that pause between steps
    private static final long LOG_QUIET_PERIOD_MS = Long.parseLong(
            System.getProperty("cpln.stalled.log.quiet.period.ms", "60000"));

    // Grace period before considering a build "stalled" (default: 90 seconds)
    // Build must be in stalled state for this long before action
    private static final long STALLED_GRACE_PERIOD_MS = Long.parseLong(
            System.getProperty("cpln.stalled.grace.period.ms", "90000"));

    // Minimum detection cycles before action (default: 3)
    private static final int MIN_DETECTION_CYCLES = Integer.parseInt(
            System.getProperty("cpln.stalled.min.cycles", "3"));

    // Channel activity threshold (default: 45 seconds)
    // If no channel activity for this long, consider it stale
    private static final long CHANNEL_STALE_THRESHOLD_MS = Long.parseLong(
            System.getProperty("cpln.stalled.channel.threshold.ms", "45000"));

    // Response time threshold in milliseconds (default: 5000ms = 5 seconds)
    // If response time exceeds this, the agent is considered degraded
    private static final long RESPONSE_TIME_THRESHOLD_MS = Long.parseLong(
            System.getProperty("cpln.stalled.response.time.threshold.ms", "5000"));

    // Track potentially stalled builds
    private final Map<String, StalledBuildInfo> potentiallyStalled = new ConcurrentHashMap<>();

    public StalledBuildDetector() {
        super("CPLN Stalled Build Detector");
    }

    @Override
    public long getRecurrencePeriod() {
        return CHECK_INTERVAL_MS;
    }

    @Override
    protected void execute(TaskListener listener) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) {
            return;
        }

        LOGGER.log(FINE, "Running stalled build detection...");

        // Process each CPLN agent
        for (Node node : jenkins.getNodes()) {
            if (!(node instanceof Agent)) {
                continue;
            }

            Agent agent = (Agent) node;
            Cloud cloud = agent.getCloud();

            if (cloud == null) {
                continue;
            }

            checkAgentForStalledBuilds(agent, cloud, jenkins);
        }

        // Clean up tracking for nodes that no longer exist
        potentiallyStalled.keySet().removeIf(name -> {
            String nodeName = name.split(":")[0]; // Format: "nodeName:executorIndex"
            return jenkins.getNode(nodeName) == null;
        });

        LOGGER.log(FINE, "Stalled build detection completed. Tracking {0} potentially stalled builds.",
                potentiallyStalled.size());
    }

    /**
     * Check an agent for stalled builds.
     */
    private void checkAgentForStalledBuilds(Agent agent, Cloud cloud, Jenkins jenkins) {
        String nodeName = agent.getNodeName();
        hudson.model.Computer computer = jenkins.getComputer(nodeName);

        if (computer == null) {
            return;
        }

        // RULE 1: Agent must be online
        // If offline, other cleanup mechanisms handle it
        if (computer.isOffline()) {
            clearStalledTracking(nodeName);
            return;
        }

        // Check each executor
        for (Executor executor : computer.getExecutors()) {
            checkExecutorForStalledBuild(executor, agent, cloud, computer, nodeName);
        }

        // Also check one-off executors (for Pipeline lightweight executors)
        for (Executor executor : computer.getOneOffExecutors()) {
            checkExecutorForStalledBuild(executor, agent, cloud, computer, nodeName);
        }
    }

    /**
     * Check a specific executor for a stalled build.
     */
    private void checkExecutorForStalledBuild(Executor executor, Agent agent, Cloud cloud,
                                               hudson.model.Computer computer, String nodeName) {
        
        // RULE 2: Executor must be busy
        // If not busy, StuckAgentDetector handles cleanup
        if (!executor.isBusy()) {
            String key = buildKey(nodeName, executor);
            potentiallyStalled.remove(key);
            LOGGER.log(FINE, "Agent {0} executor not busy, skipping stalled build check", nodeName);
            return;
        }

        Queue.Executable executable = executor.getCurrentExecutable();
        if (executable == null) {
            LOGGER.log(FINE, "Agent {0} has busy executor but no executable", nodeName);
            return;
        }

        String key = buildKey(nodeName, executor);
        String buildName = getBuildName(executable);
        
        LOGGER.log(INFO, "Checking agent {0} for stalled build: {1}", new Object[]{nodeName, buildName});

        // RULE 3: Check log quiet period
        // If logs are still being written, build is still active
        long logQuietTime = getLogQuietTime(executable);
        LOGGER.log(INFO, "Agent {0} log quiet time: {1}ms (threshold: {2}ms)",
                new Object[]{nodeName, logQuietTime, LOG_QUIET_PERIOD_MS});
        
        if (logQuietTime < LOG_QUIET_PERIOD_MS) {
            // Logs are recent, build is still producing output
            potentiallyStalled.remove(key);
            LOGGER.log(INFO, "Agent {0} has recent log output ({1}ms ago), NOT stalled",
                    new Object[]{nodeName, logQuietTime});
            return;
        }

        // RULE 4: Check channel staleness / response time
        ChannelStatus channelStatus = assessChannelStatus(computer);
        LOGGER.log(INFO, "Agent {0} channel status: {1}", new Object[]{nodeName, channelStatus});
        
        if (channelStatus == ChannelStatus.HEALTHY) {
            potentiallyStalled.remove(key);
            LOGGER.log(INFO, "Agent {0} channel is HEALTHY, NOT stalled", nodeName);
            return;
        }

        // RULE 5: Check CPLN workload is still running
        // If workload crashed, other mechanisms handle cleanup
        if (!isWorkloadRunningHealthy(cloud, nodeName)) {
            potentiallyStalled.remove(key);
            LOGGER.log(INFO, "Agent {0} workload not running healthy, skipping stalled detection", nodeName);
            return;
        }
        
        LOGGER.log(INFO, "Agent {0} MATCHES stalled criteria: logQuiet={1}ms, channelStatus={2}",
                new Object[]{nodeName, logQuietTime, channelStatus});

        // All criteria met - this build may be stalled
        StalledBuildInfo info = potentiallyStalled.computeIfAbsent(key,
                k -> new StalledBuildInfo(Instant.now(), executable));

        info.detectionCount++;
        info.lastChannelStatus = channelStatus;
        info.lastLogQuietTime = logQuietTime;

        // RULE 6: Check grace period and detection cycles
        long stalledDurationMs = Duration.between(info.firstDetected, Instant.now()).toMillis();

        if (stalledDurationMs < STALLED_GRACE_PERIOD_MS) {
            LOGGER.log(FINE, "Build on {0} potentially stalled for {1}ms, waiting for grace period ({2}ms)",
                    new Object[]{nodeName, stalledDurationMs, STALLED_GRACE_PERIOD_MS});
            return;
        }

        if (info.detectionCount < MIN_DETECTION_CYCLES) {
            LOGGER.log(FINE, "Build on {0} detected {1} times, waiting for min cycles ({2})",
                    new Object[]{nodeName, info.detectionCount, MIN_DETECTION_CYCLES});
            return;
        }

        // All criteria met for extended period - this build is stalled
        LOGGER.log(WARNING,
                "STALLED BUILD DETECTED on {0}:\n" +
                "  - Build: {1}\n" +
                "  - Stalled duration: {2}ms (threshold: {3}ms)\n" +
                "  - Detection cycles: {4} (threshold: {5})\n" +
                "  - Log quiet time: {6}ms (threshold: {7}ms)\n" +
                "  - Channel status: {8}\n" +
                "  - Executor busy: true, Workload running: true\n" +
                "  Triggering forced build completion...",
                new Object[]{
                    nodeName,
                    getBuildName(executable),
                    stalledDurationMs,
                    STALLED_GRACE_PERIOD_MS,
                    info.detectionCount,
                    MIN_DETECTION_CYCLES,
                    logQuietTime,
                    LOG_QUIET_PERIOD_MS,
                    channelStatus
                });

        // Force complete the stalled build
        forceCompleteBuild(executor, agent, cloud, nodeName, stalledDurationMs);
        potentiallyStalled.remove(key);
    }

    /**
     * Get the time since the last log output for a build.
     * 
     * @return milliseconds since last log output, or 0 if unknown
     */
    private long getLogQuietTime(Queue.Executable executable) {
        try {
            if (executable instanceof Run) {
                Run<?, ?> run = (Run<?, ?>) executable;
                File logFile = run.getLogFile();
                
                if (logFile != null && logFile.exists()) {
                    long lastModified = logFile.lastModified();
                    long quietTime = System.currentTimeMillis() - lastModified;
                    return quietTime;
                }
            }
        } catch (Exception e) {
            LOGGER.log(FINE, "Error checking log file: {0}", e.getMessage());
        }
        
        // If we can't determine, return 0 (assume recent activity)
        return 0;
    }

    /**
     * Assess the status of the computer's remoting channel.
     * Checks multiple indicators: response time, channel state, and activity tracking.
     */
    private ChannelStatus assessChannelStatus(hudson.model.Computer computer) {
        try {
            // CRITICAL CHECK: Response time from Jenkins' ResponseTimeMonitor
            // This is what Jenkins displays in the UI (e.g., "12980ms")
            long responseTime = getResponseTime(computer);
            if (responseTime > RESPONSE_TIME_THRESHOLD_MS) {
                LOGGER.log(INFO, "Agent {0} has HIGH response time: {1}ms (threshold: {2}ms)",
                        new Object[]{computer.getName(), responseTime, RESPONSE_TIME_THRESHOLD_MS});
                return ChannelStatus.HIGH_RESPONSE_TIME;
            }
            
            VirtualChannel virtualChannel = computer.getChannel();
            
            if (virtualChannel == null) {
                LOGGER.log(INFO, "Agent {0} has NO channel but appears online", computer.getName());
                return ChannelStatus.NO_CHANNEL;
            }

            if (virtualChannel instanceof Channel) {
                Channel channel = (Channel) virtualChannel;
                
                if (channel.isClosingOrClosed()) {
                    LOGGER.log(INFO, "Agent {0} channel is closing/closed", computer.getName());
                    return ChannelStatus.CLOSING;
                }

                // Try to get last activity time from channel
                long lastHeard = getChannelLastHeard(channel);
                if (lastHeard > 0) {
                    long activityAge = System.currentTimeMillis() - lastHeard;
                    if (activityAge > CHANNEL_STALE_THRESHOLD_MS) {
                        LOGGER.log(INFO, "Agent {0} channel stale: no activity for {1}ms",
                                new Object[]{computer.getName(), activityAge});
                        return ChannelStatus.STALE;
                    }
                }
            }

            // Check CPLN Computer's activity tracking
            if (computer instanceof Computer) {
                Computer cplnComputer = (Computer) computer;
                long lastActivity = cplnComputer.getLastActivityTime();
                if (lastActivity > 0) {
                    long activityAge = System.currentTimeMillis() - lastActivity;
                    if (activityAge > CHANNEL_STALE_THRESHOLD_MS) {
                        LOGGER.log(INFO, "Agent {0} CPLN activity stale: no activity for {1}ms",
                                new Object[]{computer.getName(), activityAge});
                        return ChannelStatus.STALE;
                    }
                }
            }

            LOGGER.log(FINE, "Agent {0} channel appears healthy (response time: {1}ms)",
                    new Object[]{computer.getName(), responseTime});
            return ChannelStatus.HEALTHY;

        } catch (Exception e) {
            LOGGER.log(WARNING, "Error assessing channel for {0}: {1}",
                    new Object[]{computer.getName(), e.getMessage()});
            return ChannelStatus.HEALTHY; // Assume healthy on error to avoid false positives
        }
    }

    /**
     * Get the response time from Jenkins' ResponseTimeMonitor.
     * This is the value displayed in the Jenkins UI.
     * 
     * @return response time in milliseconds, or -1 if unavailable
     */
    private long getResponseTime(hudson.model.Computer computer) {
        try {
            ResponseTimeMonitor.Data data = ResponseTimeMonitor.DESCRIPTOR.get(computer);
            if (data != null) {
                return data.getAverage();
            }
        } catch (Exception e) {
            LOGGER.log(FINE, "Error getting response time: {0}", e.getMessage());
        }
        return -1;
    }

    /**
     * Get the last time the channel received data.
     */
    private long getChannelLastHeard(Channel channel) {
        try {
            // Try getLastHeard() via reflection (version-dependent)
            java.lang.reflect.Method method = channel.getClass().getMethod("getLastHeard");
            Object result = method.invoke(channel);
            if (result instanceof Long) {
                return (Long) result;
            }
        } catch (NoSuchMethodException e) {
            // Method not available
        } catch (Exception e) {
            LOGGER.log(FINEST, "Error getting last heard: {0}", e.getMessage());
        }
        return -1;
    }

    /**
     * Check if the CPLN workload is running and healthy.
     * If it's crashed or restarting, other mechanisms handle it.
     */
    private boolean isWorkloadRunningHealthy(Cloud cloud, String workloadName) {
        try {
            WorkloadReconciler.WorkloadHealthStatus status = 
                    getWorkloadHealthStatus(cloud, workloadName);
            
            return status == WorkloadReconciler.WorkloadHealthStatus.HEALTHY ||
                   status == WorkloadReconciler.WorkloadHealthStatus.UNKNOWN;
                   
        } catch (Exception e) {
            LOGGER.log(FINE, "Error checking workload health: {0}", e.getMessage());
            return true; // Assume healthy on error
        }
    }

    /**
     * Get the health status of a workload.
     */
    private WorkloadReconciler.WorkloadHealthStatus getWorkloadHealthStatus(Cloud cloud, String workloadName) {
        try {
            java.net.http.HttpResponse<String> response = Utils.send(Utils.request(
                    String.format(io.jenkins.plugins.cpln.model.Workload.GETURI, 
                            cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            if (response.statusCode() == 404) {
                return WorkloadReconciler.WorkloadHealthStatus.NOT_FOUND;
            }
            
            if (response.statusCode() != 200) {
                return WorkloadReconciler.WorkloadHealthStatus.UNKNOWN;
            }

            io.jenkins.plugins.cpln.model.Workload workload = Utils.readGetWorkload(response.body());
            
            if (workload.status == null) {
                return WorkloadReconciler.WorkloadHealthStatus.UNKNOWN;
            }

            // Check for terminated/crashed states
            if (workload.status.health != null) {
                String health = workload.status.health.toLowerCase();
                if (health.contains("terminated") || health.contains("stopped")) {
                    return WorkloadReconciler.WorkloadHealthStatus.TERMINATED;
                }
                if (health.contains("crash") || health.contains("error") || health.contains("failed")) {
                    return WorkloadReconciler.WorkloadHealthStatus.CRASHED;
                }
            }

            // If workload is active, consider it healthy
            if (workload.status.active) {
                return WorkloadReconciler.WorkloadHealthStatus.HEALTHY;
            }

            return WorkloadReconciler.WorkloadHealthStatus.UNHEALTHY;
            
        } catch (Exception e) {
            LOGGER.log(FINE, "Error getting workload status: {0}", e.getMessage());
            return WorkloadReconciler.WorkloadHealthStatus.UNKNOWN;
        }
    }

    /**
     * Force complete a stalled build.
     * 
     * This method:
     * 1. Aborts/interrupts the stuck executor
     * 2. Removes the agent node
     * 3. Deletes the CPLN workload
     * 4. Cleans up internal state
     */
    private void forceCompleteBuild(Executor executor, Agent agent, Cloud cloud, 
                                     String nodeName, long stalledDurationMs) {
        
        String reason = String.format("stalled build completion (no log output or channel activity for %dms)", 
                stalledDurationMs);
        
        LOGGER.log(WARNING, "Force completing stalled build on {0}. Reason: {1}",
                new Object[]{nodeName, reason});

        try {
            // STEP 1: Interrupt the stuck executor
            // This sends an interrupt signal to the build
            Queue.Executable executable = executor.getCurrentExecutable();
            if (executable != null) {
                LOGGER.log(INFO, "Interrupting stalled executor for build: {0}", getBuildName(executable));
                
                // Use doStop() to interrupt the executor
                // This is the safest way to stop a running build
                executor.interrupt();
                
                // Give it a moment to process the interrupt
                Thread.sleep(2000);
                
                // If still busy, force harder interrupt
                if (executor.isBusy()) {
                    LOGGER.log(WARNING, "Executor still busy after interrupt, forcing harder stop");
                    executor.interrupt(Result.ABORTED);
                }
            }

            // STEP 2 & 3: Cleanup node and workload using centralized cleanup
            // This ensures correct order: node first, then workload
            // Note: We delay this slightly to allow the interrupt to propagate
            Thread.sleep(3000);
            
            // Check if executor is still busy (interrupt might have worked)
            if (!executor.isBusy()) {
                LOGGER.log(INFO, "Executor successfully interrupted, cleanup will proceed normally");
                // For unique agents, cleanup happens via normal mechanisms after build ends
                if (cloud.getUseUniqueAgents()) {
                    // Schedule cleanup to ensure it happens
                    CplnCleanupListener.forceCleanup(nodeName, cloud, reason);
                }
            } else {
                // Executor is still stuck - force cleanup
                LOGGER.log(WARNING, "Executor still busy, forcing cleanup for {0}", nodeName);
                CplnCleanup.cleanupNodeAndWorkload(cloud, nodeName, reason);
            }

            // STEP 4: Clean up internal tracking
            WorkloadReconciler.untrackWorkload(nodeName);
            StuckAgentDetector.reportActivity(nodeName); // Reset stuck detection
            
            LOGGER.log(INFO, "Force completion of stalled build on {0} completed", nodeName);

        } catch (Exception e) {
            LOGGER.log(WARNING, "Error during force completion for {0}: {1}",
                    new Object[]{nodeName, e.getMessage()});
        }
    }

    /**
     * Get a readable name for an executable.
     */
    private String getBuildName(Queue.Executable executable) {
        if (executable instanceof Run) {
            return ((Run<?, ?>) executable).getFullDisplayName();
        }
        return executable.toString();
    }

    /**
     * Build a tracking key for an executor.
     */
    private String buildKey(String nodeName, Executor executor) {
        return nodeName + ":" + executor.getNumber();
    }

    /**
     * Clear all stalled tracking for a node.
     */
    private void clearStalledTracking(String nodeName) {
        potentiallyStalled.keySet().removeIf(key -> key.startsWith(nodeName + ":"));
    }

    /**
     * Report activity for a node (resets stalled detection).
     * Call this when meaningful build progress occurs.
     */
    public static void reportBuildActivity(String nodeName) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return;
        
        StalledBuildDetector detector = jenkins.getExtensionList(StalledBuildDetector.class).get(0);
        if (detector != null) {
            detector.clearStalledTracking(nodeName);
        }
    }

    /**
     * Channel status enum.
     */
    private enum ChannelStatus {
        /** Channel is functioning normally with acceptable response time */
        HEALTHY,
        /** No channel available for an online agent */
        NO_CHANNEL,
        /** Channel is closing or closed */
        CLOSING,
        /** Channel has very high response time (>5s default) */
        HIGH_RESPONSE_TIME,
        /** No channel activity for extended period */
        STALE
    }

    /**
     * Information about a potentially stalled build.
     */
    private static class StalledBuildInfo {
        final Instant firstDetected;
        final Queue.Executable executable;
        ChannelStatus lastChannelStatus;
        long lastLogQuietTime;
        int detectionCount;

        StalledBuildInfo(Instant firstDetected, Queue.Executable executable) {
            this.firstDetected = firstDetected;
            this.executable = executable;
            this.detectionCount = 1;
        }
    }
}

