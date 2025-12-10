package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.slaves.AbstractCloudSlave;
import hudson.slaves.ComputerLauncher;
import org.kohsuke.stapler.DataBoundConstructor;

import java.io.IOException;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * Agent implementation for CPLN workloads.
 * 
 * This class manages the lifecycle of a Jenkins agent that runs on a CPLN workload.
 * The cleanup logic has been enhanced to be independent from the remoting lifecycle:
 * 
 * 1. _terminate() handles graceful termination triggered by Jenkins retention strategy
 * 2. WorkloadReconciler handles orphan workloads (periodic background cleanup)
 * 3. CplnCleanupListener handles abnormal disconnections and connection failures
 * 
 * This ensures workloads are cleaned up regardless of:
 * - Handshake failures
 * - ClosedChannelException
 * - Missing X-Remoting-Capability headers
 * - OOM kills
 * - Fast exits
 * - Abnormal websocket terminations
 * - Jenkins controller restarts
 */
@SuppressFBWarnings
public class Agent extends AbstractCloudSlave {

    private static final long serialVersionUID = 5165504654221829569L;

    private static final Logger LOGGER = Logger.getLogger(Agent.class.getName());

    private transient Cloud cloud;
    
    // Track the actual workload name (may differ from node name)
    private String workloadName;
    
    // Track if termination has been initiated
    private transient volatile boolean terminationInitiated = false;

    public Cloud getCloud() {
        return cloud;
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    /**
     * Get the CPLN workload name.
     * Falls back to node name if not explicitly set.
     */
    public String getWorkloadName() {
        return workloadName != null ? workloadName : getNodeName();
    }

    /**
     * Set the CPLN workload name.
     */
    public void setWorkloadName(String workloadName) {
        this.workloadName = workloadName;
    }

    @DataBoundConstructor
    public Agent(@NonNull String name, String remoteFS, ComputerLauncher launcher)
            throws Descriptor.FormException, IOException {
        super(name, remoteFS, launcher);
        this.workloadName = name;
    }

    @Override
    public Computer createComputer() {
        return new Computer(this);
    }

    /**
     * Terminate the agent and clean up the CPLN workload.
     * 
     * IMPORTANT: This method is called by Jenkins AFTER the node is already being removed.
     * The node removal is handled by Jenkins' AbstractCloudSlave, so we only need to
     * delete the CPLN workload here.
     * 
     * This method is called by Jenkins when:
     * - The retention strategy decides the agent should be removed
     * - The agent is manually deleted
     * - Jenkins is shutting down
     * 
     * The workload deletion is also handled by:
     * - CplnCleanupListener for abnormal disconnections
     * - WorkloadReconciler for orphan cleanup
     * 
     * This ensures multiple layers of cleanup protection.
     */
    @Override
    protected void _terminate(TaskListener listener) {
        if (terminationInitiated) {
            LOGGER.log(FINE, "Termination already initiated for agent {0}", getNodeName());
            return;
        }
        
        terminationInitiated = true;
        
        try {
            Cloud effectiveCloud = getCloud();
            if (effectiveCloud == null) {
                LOGGER.log(WARNING, "Cannot terminate agent {0} - cloud is null", getNodeName());
                // Still try to untrack
                WorkloadReconciler.untrackWorkload(getWorkloadName());
                return;
            }
            
            // Cancel any pending cleanup from listener (we're handling it here)
            CplnCleanupListener.cancelPendingCleanup(getWorkloadName());
            
            LOGGER.log(INFO, "Terminating agent {0} on cloud {1}, deleting workload {2}...",
                    new Object[]{getNodeName(), effectiveCloud, getWorkloadName()});
            
            // Delete the CPLN workload synchronously
            // Note: The Jenkins node is already being removed by AbstractCloudSlave
            // so we only need to delete the CPLN workload here
            CplnCleanup.deleteWorkloadSync(effectiveCloud, getWorkloadName());
            
        } finally {
            // Always untrack the workload
            WorkloadReconciler.untrackWorkload(getWorkloadName());
        }
    }

    /**
     * Check if termination has been initiated.
     */
    public boolean isTerminationInitiated() {
        return terminationInitiated;
    }

    /**
     * Force cleanup of the workload (for external callers).
     * This can be called when cleanup is needed outside the normal termination flow.
     */
    public void forceCleanup(String reason) {
        Cloud effectiveCloud = getCloud();
        if (effectiveCloud == null) {
            LOGGER.log(WARNING, "Cannot force cleanup for agent {0} - cloud is null", getNodeName());
            return;
        }
        
        LOGGER.log(INFO, "Force cleanup requested for agent {0}. Reason: {1}",
                new Object[]{getNodeName(), reason});
        
        CplnCleanupListener.forceCleanup(getWorkloadName(), effectiveCloud, reason);
    }

    @Extension
    @SuppressWarnings("unused")
    public static final class DescriptorImpl extends SlaveDescriptor {

        @Override
        @NonNull
        public String getDisplayName() {
            return "Cpln Agent";
        }

        @Override
        public boolean isInstantiable() {
            return false;
        }
    }
}
