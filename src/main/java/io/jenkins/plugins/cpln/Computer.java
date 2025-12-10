package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.slaves.AbstractCloudComputer;
import hudson.slaves.OfflineCause;
import org.kohsuke.stapler.HttpResponse;
import org.kohsuke.stapler.HttpResponses;

import java.util.concurrent.Future;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * Computer implementation for CPLN agents.
 * 
 * This class extends AbstractCloudComputer and adds CPLN-specific cleanup logic
 * that is independent from the remoting lifecycle. It ensures workload cleanup
 * happens regardless of how the agent connection terminates.
 * 
 * Also tracks activity time for stuck agent detection - this helps identify
 * agents that appear online but have a stalled JVM.
 */
@SuppressFBWarnings
public class Computer extends AbstractCloudComputer<Agent> {

    private static final Logger LOGGER = Logger.getLogger(Computer.class.getName());
    
    // Track if workload has been provisioned
    private volatile boolean workloadProvisioned = false;
    
    // Track if cleanup has been triggered
    private volatile boolean cleanupTriggered = false;
    
    // Track last activity time for stuck agent detection
    // This is updated when meaningful channel activity occurs
    private volatile long lastActivityTime = 0;

    public Computer(Agent slave) {
        super(slave);
        // Initialize activity time when computer is created
        this.lastActivityTime = System.currentTimeMillis();
    }

    /**
     * Mark that the workload has been provisioned.
     * Called by Launcher after successful workload creation.
     */
    public void setWorkloadProvisioned(boolean provisioned) {
        this.workloadProvisioned = provisioned;
        if (provisioned) {
            // Record activity when workload is provisioned
            recordActivity();
            Agent agent = getNode();
            if (agent != null && agent.getCloud() != null) {
                WorkloadReconciler.trackWorkload(getName(), agent.getCloud().name, null);
            }
        }
    }

    /**
     * Check if workload has been provisioned.
     */
    public boolean isWorkloadProvisioned() {
        return workloadProvisioned;
    }

    @Override
    @CheckForNull
    public Agent getNode() {
        return super.getNode();
    }

    /**
     * Override disconnect to ensure cleanup is triggered.
     */
    @Override
    public Future<?> disconnect(OfflineCause cause) {
        LOGGER.log(INFO, "CPLN Computer {0} disconnecting. Cause: {1}", 
                new Object[]{getName(), cause != null ? cause.toString() : "unknown"});
        
        triggerCleanup("disconnect: " + (cause != null ? cause.toString() : "unknown"));
        
        return super.disconnect(cause);
    }

    /**
     * Override doDoDelete to ensure cleanup is triggered when node is manually deleted.
     */
    @Override
    public HttpResponse doDoDelete() {
        LOGGER.log(INFO, "CPLN Computer {0} being deleted", getName());
        
        triggerCleanup("manual deletion");
        
        try {
            return super.doDoDelete();
        } catch (java.io.IOException e) {
            LOGGER.log(WARNING, "Error during doDoDelete for {0}: {1}", 
                    new Object[]{getName(), e.getMessage()});
            throw HttpResponses.error(500, e);
        }
    }

    /**
     * Trigger cleanup of the associated workload.
     * This is called from multiple points to ensure cleanup happens.
     */
    public synchronized void triggerCleanup(String reason) {
        if (cleanupTriggered) {
            LOGGER.log(FINE, "Cleanup already triggered for {0}", getName());
            return;
        }
        
        Agent agent = getNode();
        if (agent == null) {
            LOGGER.log(WARNING, "Cannot trigger cleanup - agent is null for computer {0}", getName());
            return;
        }
        
        Cloud cloud = agent.getCloud();
        if (cloud == null) {
            LOGGER.log(WARNING, "Cannot trigger cleanup - cloud is null for agent {0}", getName());
            return;
        }
        
        // Only trigger cleanup if workload was provisioned
        if (!workloadProvisioned) {
            LOGGER.log(FINE, "Skipping cleanup for {0} - workload not provisioned", getName());
            return;
        }
        
        cleanupTriggered = true;
        
        LOGGER.log(INFO, "Triggering workload cleanup for {0}. Reason: {1}",
                new Object[]{getName(), reason});
        
        // Use the cleanup listener for delayed/coordinated cleanup
        // This handles cases where the agent might reconnect
        if (cloud.getUseUniqueAgents()) {
            // For unique agents, cleanup via CplnCleanupListener which handles coordination
            // The listener will be notified via onOffline event
            LOGGER.log(FINE, "Unique agent cleanup will be handled by CplnCleanupListener");
        } else {
            // For shared agents, just untrack
            WorkloadReconciler.untrackWorkload(getName());
        }
    }

    /**
     * Reset cleanup state (for agent reconnection scenarios).
     */
    public void resetCleanupState() {
        cleanupTriggered = false;
    }

    /**
     * Record that meaningful activity has occurred on this agent.
     * This resets the stuck agent detection timer.
     * 
     * Called when:
     * - Agent connects/reconnects
     * - Build starts executing
     * - Channel receives meaningful communication
     */
    public void recordActivity() {
        this.lastActivityTime = System.currentTimeMillis();
        LOGGER.log(FINE, "Activity recorded for computer {0}", getName());
    }

    /**
     * Get the last activity time for stuck agent detection.
     * 
     * @return timestamp of last activity, or 0 if never recorded
     */
    public long getLastActivityTime() {
        return lastActivityTime;
    }

    /**
     * Called when the computer's channel is closed.
     * This catches abnormal terminations that bypass disconnect().
     */
    @Override
    protected void onRemoved() {
        LOGGER.log(INFO, "CPLN Computer {0} removed", getName());
        
        // Final cleanup attempt
        Agent agent = getNode();
        if (agent != null && agent.getCloud() != null && workloadProvisioned) {
            Cloud cloud = agent.getCloud();
            if (cloud.getUseUniqueAgents()) {
                CplnCleanupListener.forceCleanup(getName(), cloud, "computer removed");
            }
        }
        
        WorkloadReconciler.untrackWorkload(getName());
        super.onRemoved();
    }
}
