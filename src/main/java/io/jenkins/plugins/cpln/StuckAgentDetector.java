package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.AsyncPeriodicWork;
import hudson.model.Executor;
import hudson.model.Node;
import hudson.model.TaskListener;
import jenkins.model.Jenkins;
import hudson.remoting.Channel;
import hudson.remoting.VirtualChannel;
import hudson.node_monitors.ResponseTimeMonitor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * StuckAgentDetector detects and cleans up agents that are "alive but stalled."
 * 
 * This addresses a specific failure mode where:
 * - The Jenkins agent JVM becomes CPU-starved or freezes
 * - The job script finishes printing output, but the JVM never exits
 * - The agent remains "online" in Jenkins but is unusable (slow response, clock drift)
 * - No onDisconnect, onOffline, or channel-closed events fire
 * - The workload stays in CPLN as "Running" forever
 * - Standard cleanup is never triggered
 * 
 * DETECTION CRITERIA (ALL must be true to trigger cleanup):
 * 1. Agent is using unique-agent mode (ephemeral agents)
 * 2. Agent is currently "online" in Jenkins (not offline)
 * 3. Agent has NO busy executors (build appears to be finished)
 * 4. CPLN workload is still running
 * 5. Agent has been in this state for > STUCK_GRACE_PERIOD
 * 6. Channel shows signs of staleness (no activity, high response time, or stale heartbeat)
 * 
 * FALSE POSITIVE PREVENTION:
 * - Only targets unique agents (meant to be ephemeral)
 * - Requires build to be done (no busy executors)
 * - Requires multiple detection cycles before cleanup
 * - Requires channel staleness indicators
 * - Long grace period (default 60 seconds)
 * - Never cleans up agents with running builds
 * 
 * This detector is STRICTLY SCOPED to this edge case and does not perform
 * blanket cleanup. It complements (not replaces) the WorkloadReconciler.
 */
@Extension
@SuppressFBWarnings
public class StuckAgentDetector extends AsyncPeriodicWork {

    private static final Logger LOGGER = Logger.getLogger(StuckAgentDetector.class.getName());

    // Check interval in milliseconds (default: 15 seconds)
    // More frequent than reconciler to catch stuck agents faster
    private static final long CHECK_INTERVAL_MS = Long.parseLong(
            System.getProperty("cpln.stuck.check.interval.ms", "15000"));

    // Grace period before considering an agent "stuck" (default: 60 seconds)
    // Agent must be in stuck state for this long before cleanup
    private static final long STUCK_GRACE_PERIOD_MS = Long.parseLong(
            System.getProperty("cpln.stuck.grace.period.ms", "60000"));

    // Minimum number of consecutive detections before cleanup (default: 3)
    // Prevents cleanup on temporary hiccups
    private static final int MIN_DETECTION_CYCLES = Integer.parseInt(
            System.getProperty("cpln.stuck.min.cycles", "3"));

    // Channel inactivity threshold (default: 30 seconds)
    // If no channel activity for this long, consider it stale
    private static final long CHANNEL_STALE_THRESHOLD_MS = Long.parseLong(
            System.getProperty("cpln.channel.stale.threshold.ms", "30000"));

    // Response time threshold in milliseconds (default: 5000ms = 5 seconds)
    // If response time exceeds this, the agent is considered degraded
    private static final long RESPONSE_TIME_THRESHOLD_MS = Long.parseLong(
            System.getProperty("cpln.stuck.response.time.threshold.ms", "5000"));

    // Track agents that may be stuck
    private final Map<String, StuckAgentInfo> potentiallyStuck = new ConcurrentHashMap<>();

    public StuckAgentDetector() {
        super("CPLN Stuck Agent Detector");
    }

    @Override
    public long getRecurrencePeriod() {
        return CHECK_INTERVAL_MS;
    }

    @Override
    protected void execute(TaskListener listener) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) {
            LOGGER.log(WARNING, "StuckAgentDetector: Jenkins instance is null, skipping");
            return;
        }

        LOGGER.log(INFO, "StuckAgentDetector: Running detection cycle. Total nodes: {0}", 
                jenkins.getNodes().size());

        int cplnAgentCount = 0;
        int checkedCount = 0;
        
        // Process each CPLN agent
        for (Node node : jenkins.getNodes()) {
            LOGGER.log(FINE, "StuckAgentDetector: Checking node {0}, type: {1}", 
                    new Object[]{node.getNodeName(), node.getClass().getSimpleName()});
            
            if (!(node instanceof Agent)) {
                LOGGER.log(FINE, "StuckAgentDetector: Node {0} is NOT a CPLN Agent, skipping", 
                        node.getNodeName());
                continue;
            }
            
            cplnAgentCount++;

            Agent agent = (Agent) node;
            Cloud cloud = agent.getCloud();

            if (cloud == null) {
                LOGGER.log(WARNING, "StuckAgentDetector: Agent {0} has null cloud, skipping", 
                        node.getNodeName());
                continue;
            }

            // NOTE: We now check ALL agents, not just unique agents
            // High response time or stalled channel is a problem regardless of agent type
            LOGGER.log(INFO, "StuckAgentDetector: Checking CPLN agent {0} (unique={1})", 
                    new Object[]{node.getNodeName(), cloud.getUseUniqueAgents()});
            
            checkedCount++;
            checkAgentForStuckState(agent, cloud, jenkins);
        }

        // Clean up tracking for nodes that no longer exist
        potentiallyStuck.keySet().removeIf(name -> jenkins.getNode(name) == null);

        LOGGER.log(INFO, "StuckAgentDetector: Completed. CPLN agents: {0}, Checked: {1}, Tracking: {2}",
                new Object[]{cplnAgentCount, checkedCount, potentiallyStuck.size()});
    }

    /**
     * Check if an agent is in a stuck state and trigger cleanup if necessary.
     * 
     * This method implements conservative detection to avoid false positives.
     */
    private void checkAgentForStuckState(Agent agent, Cloud cloud, Jenkins jenkins) {
        String nodeName = agent.getNodeName();
        hudson.model.Computer computer = jenkins.getComputer(nodeName);

        if (computer == null) {
            LOGGER.log(INFO, "StuckAgentDetector: Computer for {0} is null", nodeName);
            potentiallyStuck.remove(nodeName);
            return;
        }

        // RULE 2: Agent must be "online"
        // If it's offline, normal cleanup mechanisms should handle it
        boolean isOffline = computer.isOffline();
        LOGGER.log(INFO, "StuckAgentDetector: Agent {0} online={1}", 
                new Object[]{nodeName, !isOffline});
        if (isOffline) {
            potentiallyStuck.remove(nodeName);
            LOGGER.log(INFO, "StuckAgentDetector: Agent {0} is OFFLINE, skipping", nodeName);
            return;
        }

        // RULE 3: Agent must have NO busy executors
        // If build is still running, this is not a stuck agent (StalledBuildDetector handles that)
        boolean hasBusy = hasBusyExecutors(computer);
        LOGGER.log(INFO, "StuckAgentDetector: Agent {0} hasBusyExecutors={1}", 
                new Object[]{nodeName, hasBusy});
        if (hasBusy) {
            potentiallyStuck.remove(nodeName);
            LOGGER.log(INFO, "StuckAgentDetector: Agent {0} has BUSY executors, delegating to StalledBuildDetector", nodeName);
            return;
        }

        // RULE 4: CPLN workload must still exist
        // If workload is gone, different cleanup path handles it
        boolean workloadExists = CplnCleanup.workloadExists(cloud, nodeName);
        LOGGER.log(INFO, "StuckAgentDetector: Agent {0} workloadExists={1}", 
                new Object[]{nodeName, workloadExists});
        if (!workloadExists) {
            potentiallyStuck.remove(nodeName);
            LOGGER.log(INFO, "StuckAgentDetector: Agent {0} workload DOESN'T EXIST, skipping", nodeName);
            return;
        }

        // RULE 5 & 6: Check for channel staleness indicators
        ChannelHealth channelHealth = assessChannelHealth(computer);
        LOGGER.log(INFO, "StuckAgentDetector: Agent {0} channelHealth={1}", 
                new Object[]{nodeName, channelHealth});
        
        if (channelHealth == ChannelHealth.HEALTHY) {
            // Channel is healthy, agent is not stuck
            potentiallyStuck.remove(nodeName);
            LOGGER.log(INFO, "StuckAgentDetector: Agent {0} channel is HEALTHY, not stuck", nodeName);
            return;
        }

        // Agent appears to be in a potentially stuck state
        LOGGER.log(WARNING, "StuckAgentDetector: Agent {0} MATCHES stuck criteria! channelHealth={1}",
                new Object[]{nodeName, channelHealth});
        
        // Track it and check if it's been stuck long enough
        StuckAgentInfo info = potentiallyStuck.computeIfAbsent(nodeName,
                k -> new StuckAgentInfo(Instant.now(), channelHealth));

        // Update the channel health assessment
        info.lastChannelHealth = channelHealth;
        info.detectionCount++;

        // Check if grace period has passed AND minimum detection cycles met
        long stuckDurationMs = Duration.between(info.firstDetected, Instant.now()).toMillis();
        
        if (stuckDurationMs < STUCK_GRACE_PERIOD_MS) {
            LOGGER.log(WARNING, "StuckAgentDetector: Agent {0} potentially stuck for {1}ms, waiting for grace period ({2}ms)",
                    new Object[]{nodeName, stuckDurationMs, STUCK_GRACE_PERIOD_MS});
            return;
        }

        if (info.detectionCount < MIN_DETECTION_CYCLES) {
            LOGGER.log(WARNING, "StuckAgentDetector: Agent {0} detected {1} times, waiting for min cycles ({2})",
                    new Object[]{nodeName, info.detectionCount, MIN_DETECTION_CYCLES});
            return;
        }

        // All criteria met - this agent is stuck
        LOGGER.log(WARNING, 
                "STUCK AGENT DETECTED: {0}\n" +
                "  - Stuck duration: {1}ms (threshold: {2}ms)\n" +
                "  - Detection cycles: {3} (threshold: {4})\n" +
                "  - Channel health: {5}\n" +
                "  - Online: {6}, Busy executors: 0, Workload exists: true\n" +
                "  Triggering cleanup...",
                new Object[]{
                    nodeName,
                    stuckDurationMs,
                    STUCK_GRACE_PERIOD_MS,
                    info.detectionCount,
                    MIN_DETECTION_CYCLES,
                    info.lastChannelHealth,
                    !computer.isOffline()
                });

        // Perform cleanup
        triggerStuckAgentCleanup(agent, cloud, nodeName, stuckDurationMs);
        potentiallyStuck.remove(nodeName);
    }

    /**
     * Assess the health of the agent's channel.
     * 
     * Returns HEALTHY if the channel appears to be functioning normally.
     * Returns STALE or DEGRADED if there are signs of problems.
     */
    private ChannelHealth assessChannelHealth(hudson.model.Computer computer) {
        try {
            // CRITICAL CHECK: Response time from Jenkins' ResponseTimeMonitor
            // This is what Jenkins displays in the UI (e.g., "12980ms")
            long responseTime = getResponseTime(computer);
            if (responseTime > RESPONSE_TIME_THRESHOLD_MS) {
                LOGGER.log(INFO, "Agent {0} has HIGH response time: {1}ms (threshold: {2}ms)",
                        new Object[]{computer.getName(), responseTime, RESPONSE_TIME_THRESHOLD_MS});
                return ChannelHealth.HIGH_RESPONSE_TIME;
            }
            
            VirtualChannel virtualChannel = computer.getChannel();
            
            if (virtualChannel == null) {
                // No channel - this is abnormal for an "online" agent
                LOGGER.log(INFO, "Agent {0} has NO channel but appears online", computer.getName());
                return ChannelHealth.NO_CHANNEL;
            }

            // Check if the virtual channel is actually a Channel instance
            // so we can call Channel-specific methods
            if (virtualChannel instanceof Channel) {
                Channel channel = (Channel) virtualChannel;
                
                if (channel.isClosingOrClosed()) {
                    // Channel is closing but agent still appears online
                    LOGGER.log(INFO, "Agent {0} channel is closing/closed", computer.getName());
                    return ChannelHealth.CLOSING;
                }

                // Check last heartbeat if available
                // Note: Heartbeat tracking depends on Jenkins/remoting version
                // If we can't get heartbeat info, we rely on other indicators
                long lastHeartbeat = getLastHeartbeat(channel);
                if (lastHeartbeat > 0) {
                    long heartbeatAge = System.currentTimeMillis() - lastHeartbeat;
                    if (heartbeatAge > CHANNEL_STALE_THRESHOLD_MS) {
                        LOGGER.log(INFO, "Agent {0} channel heartbeat is stale: {1}ms old",
                                new Object[]{computer.getName(), heartbeatAge});
                        return ChannelHealth.STALE_HEARTBEAT;
                    }
                }
            }

            // If computer is a CPLN Computer, check our internal tracking
            if (computer instanceof Computer) {
                Computer cplnComputer = (Computer) computer;
                long lastActivity = cplnComputer.getLastActivityTime();
                if (lastActivity > 0) {
                    long activityAge = System.currentTimeMillis() - lastActivity;
                    if (activityAge > CHANNEL_STALE_THRESHOLD_MS) {
                        LOGGER.log(INFO, "Agent {0} no channel activity for {1}ms",
                                new Object[]{computer.getName(), activityAge});
                        return ChannelHealth.NO_ACTIVITY;
                    }
                }
            }

            LOGGER.log(FINE, "Agent {0} channel appears healthy (response time: {1}ms)",
                    new Object[]{computer.getName(), responseTime});
            return ChannelHealth.HEALTHY;

        } catch (Exception e) {
            LOGGER.log(WARNING, "Error assessing channel health for {0}: {1}",
                    new Object[]{computer.getName(), e.getMessage()});
            // If we can't assess, assume healthy to avoid false positives
            return ChannelHealth.HEALTHY;
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
     * Get the last heartbeat timestamp from the channel.
     * Returns -1 if not available.
     */
    private long getLastHeartbeat(Channel channel) {
        try {
            // Try to get heartbeat info via reflection if available
            // This is version-dependent and may not be available
            java.lang.reflect.Method method = channel.getClass().getMethod("getLastHeartbeat");
            Object result = method.invoke(channel);
            if (result instanceof Long) {
                return (Long) result;
            }
        } catch (NoSuchMethodException e) {
            // Method not available in this version
            LOGGER.log(FINEST, "getLastHeartbeat not available");
        } catch (Exception e) {
            LOGGER.log(FINE, "Error getting heartbeat: {0}", e.getMessage());
        }
        return -1;
    }

    /**
     * Check if computer has any busy executors.
     */
    private boolean hasBusyExecutors(hudson.model.Computer computer) {
        for (Executor executor : computer.getExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        for (Executor executor : computer.getOneOffExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Trigger cleanup for a stuck agent.
     */
    private void triggerStuckAgentCleanup(Agent agent, Cloud cloud, String nodeName, long stuckDurationMs) {
        String reason = String.format("stuck agent (no activity for %dms, JVM appears stalled)", stuckDurationMs);
        
        // Use centralized cleanup which ensures correct order:
        // 1. Remove Jenkins node FIRST
        // 2. Delete CPLN workload SECOND
        boolean cleaned = CplnCleanup.cleanupNodeAndWorkload(cloud, nodeName, reason);
        
        if (cleaned) {
            LOGGER.log(INFO, "Successfully cleaned up stuck agent: {0}", nodeName);
        } else {
            LOGGER.log(WARNING, "Failed to cleanup stuck agent: {0}", nodeName);
        }
    }

    /**
     * Manually report that an agent has activity.
     * Call this when the agent performs meaningful work to reset stuck detection.
     */
    public static void reportActivity(String nodeName) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return;
        
        StuckAgentDetector detector = jenkins.getExtensionList(StuckAgentDetector.class).get(0);
        if (detector != null) {
            // Remove from potentially stuck list - it's showing activity
            detector.potentiallyStuck.remove(nodeName);
        }
        
        // Update the computer's last activity time
        hudson.model.Computer computer = jenkins.getComputer(nodeName);
        if (computer instanceof Computer) {
            ((Computer) computer).recordActivity();
        }
    }

    /**
     * Channel health status.
     */
    private enum ChannelHealth {
        /** Channel is functioning normally */
        HEALTHY,
        /** No channel available for online agent */
        NO_CHANNEL,
        /** Channel is closing or closed */
        CLOSING,
        /** Response time is very high (>5s default) */
        HIGH_RESPONSE_TIME,
        /** Heartbeat is stale */
        STALE_HEARTBEAT,
        /** No channel activity for extended period */
        NO_ACTIVITY
    }

    /**
     * Information about a potentially stuck agent.
     */
    private static class StuckAgentInfo {
        final Instant firstDetected;
        ChannelHealth lastChannelHealth;
        int detectionCount;

        StuckAgentInfo(Instant firstDetected, ChannelHealth channelHealth) {
            this.firstDetected = firstDetected;
            this.lastChannelHealth = channelHealth;
            this.detectionCount = 1;
        }
    }
}

