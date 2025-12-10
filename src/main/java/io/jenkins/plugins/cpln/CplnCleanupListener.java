package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.Executor;
import hudson.model.TaskListener;
import hudson.slaves.ComputerListener;
import hudson.slaves.OfflineCause;
import jenkins.model.Jenkins;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * CplnCleanupListener handles cleanup of CPLN workloads on all computer lifecycle events.
 * 
 * This listener is designed to catch cleanup opportunities that the normal Jenkins
 * agent lifecycle might miss, including:
 * - Connection failures during handshake
 * - Abnormal disconnections (ClosedChannelException)
 * - Missing X-Remoting-Capability headers
 * - OOM kills on the agent side
 * - Fast exits
 * - WebSocket terminations
 * 
 * Unlike relying solely on Agent._terminate(), this listener:
 * - Triggers cleanup on ANY offline event, not just graceful termination
 * - Schedules delayed cleanup for transient failures
 * - Integrates with the WorkloadReconciler for orphan cleanup
 * - NEVER deletes agents that have active/busy executors
 */
@Extension
@SuppressFBWarnings
public class CplnCleanupListener extends ComputerListener {

    private static final Logger LOGGER = Logger.getLogger(CplnCleanupListener.class.getName());
    
    // Delayed cleanup to handle transient failures
    private static final long CLEANUP_DELAY_SECONDS = Long.parseLong(
            System.getProperty("cpln.cleanup.delay.seconds", "30"));
    
    // Executor for delayed cleanup tasks
    private static final ScheduledExecutorService cleanupExecutor = 
            new ScheduledThreadPoolExecutor(2, r -> {
                Thread t = new Thread(r, "cpln-cleanup-executor");
                t.setDaemon(true);
                return t;
            });
    
    // Track pending cleanups to avoid duplicates
    private static final ConcurrentHashMap<String, Long> pendingCleanups = new ConcurrentHashMap<>();

    /**
     * Called when a computer comes online.
     * We use this to clear any pending cleanup tasks since the agent is now connected.
     */
    @Override
    public void onOnline(hudson.model.Computer c, TaskListener listener) throws IOException, InterruptedException {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        String workloadName = agent.getNodeName();
        Cloud cloud = agent.getCloud();
        
        // Cancel any pending cleanup since the agent came online
        pendingCleanups.remove(workloadName);
        
        // Record activity for stuck agent detection
        // Agent just came online, so it's clearly not stuck
        computer.recordActivity();
        
        // Clear the provisioning cooldown so new agents can be provisioned for other jobs
        Cloud.clearProvisioningCooldown(cloud.name, agent.getLabelString());
        
        LOGGER.log(INFO, "CPLN agent {0} came online, cancelled any pending cleanup and cleared provisioning cooldown", workloadName);
        
        // Update workload tracking
        WorkloadReconciler.trackWorkload(workloadName, cloud.name, null);
    }

    /**
     * Called when a computer goes offline.
     * This is where we trigger cleanup regardless of the offline cause.
     */
    @Override
    public void onOffline(@NonNull hudson.model.Computer c, OfflineCause cause) {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        // Only cleanup if the workload was actually provisioned
        // This prevents cleanup during initial startup when the agent hasn't connected yet
        if (!computer.isWorkloadProvisioned()) {
            LOGGER.log(FINE, "Ignoring offline event for {0} - workload not yet provisioned", 
                    computer.getName());
            return;
        }
        
        // CRITICAL: Never cleanup if there are busy executors (running builds)
        if (hasBusyExecutors(c)) {
            LOGGER.log(INFO, "Ignoring offline event for {0} - agent has busy executors", 
                    computer.getName());
            return;
        }
        
        String workloadName = agent.getNodeName();
        Cloud cloud = agent.getCloud();
        
        LOGGER.log(INFO, "CPLN agent {0} went offline. Cause: {1}", 
                new Object[]{workloadName, cause != null ? cause.toString() : "unknown"});
        
        // For unique agents, always schedule cleanup
        // For shared agents, only cleanup if the cause indicates a permanent failure
        if (cloud.getUseUniqueAgents() || isPermanentOfflineCause(cause)) {
            scheduleCleanup(workloadName, cloud, "agent went offline: " + 
                    (cause != null ? cause.toString() : "unknown cause"));
        }
    }

    /**
     * Called when a computer fails to launch.
     * Only cleanup if the workload was actually created in CPLN.
     */
    @Override
    public void onLaunchFailure(hudson.model.Computer c, TaskListener listener) throws IOException, InterruptedException {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        // Only cleanup if workload was actually provisioned in CPLN
        // This prevents premature cleanup during normal startup
        if (!computer.isWorkloadProvisioned()) {
            LOGGER.log(FINE, "Ignoring launch failure for {0} - workload not yet provisioned", 
                    computer.getName());
            return;
        }
        
        // CRITICAL: Never cleanup if there are busy executors
        if (hasBusyExecutors(c)) {
            LOGGER.log(INFO, "Ignoring launch failure for {0} - agent has busy executors", 
                    computer.getName());
            return;
        }
        
        String workloadName = agent.getNodeName();
        Cloud cloud = agent.getCloud();
        
        LOGGER.log(WARNING, "CPLN agent {0} launch failed, scheduling cleanup", workloadName);
        
        // Schedule cleanup with delay to allow for transient failures
        scheduleCleanup(workloadName, cloud, "launch failure");
    }

    /**
     * Called when computer configuration is updated (including removal).
     * We intentionally do NOT force reconciliation here as it can interfere
     * with normal node creation. The periodic WorkloadReconciler handles
     * orphan cleanup on its own schedule.
     */
    @Override
    public void onConfigurationChange() {
        // Do nothing - let the periodic reconciler handle orphan cleanup
        // Forcing reconciliation here can interfere with node creation
        LOGGER.log(FINE, "Configuration change detected, periodic reconciler will handle cleanup");
    }

    /**
     * Check if a computer has any busy executors (running builds).
     */
    private boolean hasBusyExecutors(hudson.model.Computer computer) {
        if (computer == null) return false;
        
        for (Executor executor : computer.getExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        
        // Also check one-off executors
        for (Executor executor : computer.getOneOffExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Schedule cleanup of a workload with default delay.
     */
    private void scheduleCleanup(String workloadName, Cloud cloud, String reason) {
        scheduleCleanup(workloadName, cloud, reason, CLEANUP_DELAY_SECONDS);
    }

    /**
     * Schedule cleanup of a workload with specified delay.
     */
    private void scheduleCleanup(String workloadName, Cloud cloud, String reason, long delaySeconds) {
        // Check if cleanup is already pending
        Long existingCleanup = pendingCleanups.get(workloadName);
        if (existingCleanup != null) {
            LOGGER.log(FINE, "Cleanup already scheduled for workload {0}", workloadName);
            return;
        }
        
        long scheduledTime = System.currentTimeMillis() + (delaySeconds * 1000);
        pendingCleanups.put(workloadName, scheduledTime);
        
        LOGGER.log(INFO, "Scheduling cleanup of workload {0} in {1} seconds. Reason: {2}",
                new Object[]{workloadName, delaySeconds, reason});
        
        cleanupExecutor.schedule(() -> {
            executeCleanup(workloadName, cloud, reason);
        }, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Execute the actual cleanup.
     * IMPORTANT: Uses CplnCleanup to ensure correct order: node first, then workload.
     */
    private void executeCleanup(String workloadName, Cloud cloud, String reason) {
        // Check if cleanup is still needed (agent might have reconnected)
        Long scheduledTime = pendingCleanups.get(workloadName);
        if (scheduledTime == null) {
            LOGGER.log(FINE, "Cleanup cancelled for workload {0} - no longer pending", workloadName);
            return;
        }
        
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins != null) {
            hudson.model.Computer computer = jenkins.getComputer(workloadName);
            
            // Check if agent reconnected
            if (computer != null && computer.isOnline()) {
                LOGGER.log(INFO, "Skipping cleanup for workload {0} - agent is back online", workloadName);
                pendingCleanups.remove(workloadName);
                return;
            }
            
            // CRITICAL: Check if agent has busy executors (running builds)
            if (computer != null && hasBusyExecutors(computer)) {
                LOGGER.log(INFO, "Skipping cleanup for workload {0} - agent has busy executors (running builds)", workloadName);
                pendingCleanups.remove(workloadName);
                // Reschedule cleanup for later
                scheduleCleanup(workloadName, cloud, reason + " (rescheduled - had busy executors)");
                return;
            }
            
            // Check if there are queued builds waiting for this agent
            if (computer != null && hasQueuedBuilds(computer)) {
                LOGGER.log(INFO, "Skipping cleanup for workload {0} - there are builds queued for this agent", workloadName);
                pendingCleanups.remove(workloadName);
                // Reschedule cleanup for later
                scheduleCleanup(workloadName, cloud, reason + " (rescheduled - had queued builds)");
                return;
            }
        }
        
        LOGGER.log(INFO, "Executing cleanup for workload {0}. Reason: {1}",
                new Object[]{workloadName, reason});
        
        try {
            // CRITICAL: Use centralized cleanup which ensures correct order:
            // 1. Remove Jenkins node FIRST (prevents scheduling on stale node)
            // 2. Delete CPLN workload SECOND
            CplnCleanup.cleanupNodeAndWorkload(cloud, workloadName, reason);
        } finally {
            pendingCleanups.remove(workloadName);
        }
    }
    
    /**
     * Check if there are queued builds that could run on this computer.
     */
    private boolean hasQueuedBuilds(hudson.model.Computer computer) {
        if (computer == null) return false;
        
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return false;
        
        hudson.model.Node node = computer.getNode();
        if (node == null) return false;
        
        // Check the build queue for items that can run on this node
        for (hudson.model.Queue.Item item : jenkins.getQueue().getItems()) {
            if (item.getAssignedLabel() == null) {
                // Unlabeled job - could potentially run on this node
                return true;
            }
            if (item.getAssignedLabel().matches(node)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Determine if an offline cause indicates a permanent failure that requires cleanup.
     */
    private boolean isPermanentOfflineCause(OfflineCause cause) {
        if (cause == null) {
            return true; // Unknown cause, be safe and clean up
        }
        
        String causeString = cause.toString().toLowerCase();
        
        // List of causes that indicate permanent failure
        return causeString.contains("channel") ||
               causeString.contains("disconnect") ||
               causeString.contains("terminated") ||
               causeString.contains("closed") ||
               causeString.contains("timeout") ||
               causeString.contains("error") ||
               causeString.contains("failed") ||
               causeString.contains("handshake") ||
               causeString.contains("oom") ||
               causeString.contains("kill") ||
               cause instanceof OfflineCause.ChannelTermination;
    }

    /**
     * Force immediate cleanup of a workload (for external callers).
     * Will NOT cleanup if the agent has busy executors.
     * Uses CplnCleanup to ensure correct order: node first, then workload.
     */
    public static void forceCleanup(String workloadName, Cloud cloud, String reason) {
        // First check if agent has busy executors
        if (CplnCleanup.hasBusyExecutors(workloadName)) {
            LOGGER.log(WARNING, "Refusing force cleanup for workload {0} - agent has busy executors", workloadName);
            return;
        }
        
        pendingCleanups.remove(workloadName);
        
        LOGGER.log(INFO, "Force cleanup requested for workload {0}. Reason: {1}",
                new Object[]{workloadName, reason});
        
        // Execute cleanup synchronously to ensure it completes before returning
        // This prevents race conditions where new builds are scheduled on stale nodes
        CplnCleanup.cleanupNodeAndWorkload(cloud, workloadName, reason);
    }

    /**
     * Cancel any pending cleanup for a workload.
     */
    public static void cancelPendingCleanup(String workloadName) {
        if (pendingCleanups.remove(workloadName) != null) {
            LOGGER.log(FINE, "Cancelled pending cleanup for workload {0}", workloadName);
        }
    }

    /**
     * Check if cleanup is pending for a workload.
     */
    public static boolean isCleanupPending(String workloadName) {
        return pendingCleanups.containsKey(workloadName);
    }
}
