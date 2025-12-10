package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.model.Executor;
import io.jenkins.plugins.cpln.model.Workload;
import jenkins.model.Jenkins;

import java.net.http.HttpResponse;
import java.util.logging.Logger;

import static io.jenkins.plugins.cpln.Utils.*;
import static java.util.logging.Level.*;

/**
 * Centralized cleanup utility for CPLN workloads and Jenkins nodes.
 * 
 * This class implements the CORRECT cleanup order to prevent race conditions:
 * 1. Remove Jenkins node FIRST (synchronously, blocking)
 * 2. Delete CPLN workload SECOND (synchronously, blocking)
 * 
 * This prevents Jenkins from scheduling new builds on stale nodes whose
 * workloads have been deleted.
 * 
 * IMPORTANT: All cleanup paths (normal, failure, abort, timeout, OOM, fast-exit)
 * MUST use this class to ensure consistent cleanup order.
 */
@SuppressFBWarnings
public class CplnCleanup {

    private static final Logger LOGGER = Logger.getLogger(CplnCleanup.class.getName());

    /**
     * Perform full cleanup: remove Jenkins node first, then delete CPLN workload.
     * This is the ONLY correct way to cleanup - all other methods should call this.
     * 
     * @param cloud The CPLN cloud configuration
     * @param nodeName The name of the Jenkins node / CPLN workload
     * @param reason The reason for cleanup (for logging)
     * @return true if cleanup was successful
     */
    public static boolean cleanupNodeAndWorkload(Cloud cloud, String nodeName, String reason) {
        LOGGER.log(INFO, "Starting cleanup for {0}. Reason: {1}", new Object[]{nodeName, reason});
        
        // Check for busy executors before cleanup
        if (hasBusyExecutors(nodeName)) {
            LOGGER.log(WARNING, "Cannot cleanup {0} - agent has busy executors (running builds)", nodeName);
            return false;
        }
        
        boolean nodeRemoved = false;
        boolean workloadDeleted = false;
        
        try {
            // STEP 1: Remove Jenkins node FIRST (synchronous, blocking)
            // This prevents Jenkins from scheduling new builds on this node
            nodeRemoved = removeNodeSync(nodeName);
            
            // STEP 2: Delete CPLN workload SECOND (synchronous, blocking)
            // Only after the node is gone do we delete the workload
            if (cloud != null) {
                workloadDeleted = deleteWorkloadSync(cloud, nodeName);
            } else {
                LOGGER.log(WARNING, "Cloud is null, cannot delete workload {0}", nodeName);
                workloadDeleted = true; // Consider it success if we can't delete
            }
            
            // STEP 3: Untrack from reconciler
            WorkloadReconciler.untrackWorkload(nodeName);
            
            LOGGER.log(INFO, "Cleanup completed for {0}. Node removed: {1}, Workload deleted: {2}",
                    new Object[]{nodeName, nodeRemoved, workloadDeleted});
            
            return nodeRemoved || workloadDeleted;
            
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error during cleanup for {0}: {1}",
                    new Object[]{nodeName, e.getMessage()});
            return false;
        }
    }

    /**
     * Remove a Jenkins node synchronously and blocking.
     * This MUST complete before any workload deletion.
     * 
     * @param nodeName The name of the node to remove
     * @return true if node was removed or didn't exist
     */
    public static boolean removeNodeSync(String nodeName) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) {
            LOGGER.log(WARNING, "Jenkins instance not available, cannot remove node {0}", nodeName);
            return false;
        }
        
        hudson.model.Node node = jenkins.getNode(nodeName);
        if (node == null) {
            LOGGER.log(FINE, "Node {0} does not exist, nothing to remove", nodeName);
            return true;
        }
        
        try {
            // Check for busy executors one more time
            hudson.model.Computer computer = node.toComputer();
            if (computer != null) {
                for (Executor executor : computer.getExecutors()) {
                    if (executor.isBusy()) {
                        LOGGER.log(WARNING, "Cannot remove node {0} - has busy executor", nodeName);
                        return false;
                    }
                }
                for (Executor executor : computer.getOneOffExecutors()) {
                    if (executor.isBusy()) {
                        LOGGER.log(WARNING, "Cannot remove node {0} - has busy one-off executor", nodeName);
                        return false;
                    }
                }
            }
            
            // Remove the node synchronously
            LOGGER.log(INFO, "Removing Jenkins node {0} synchronously", nodeName);
            jenkins.removeNode(node);
            LOGGER.log(INFO, "Successfully removed Jenkins node {0}", nodeName);
            return true;
            
        } catch (Exception e) {
            LOGGER.log(WARNING, "Failed to remove Jenkins node {0}: {1}",
                    new Object[]{nodeName, e.getMessage()});
            return false;
        }
    }

    /**
     * Delete a CPLN workload synchronously and blocking.
     * This should ONLY be called AFTER the Jenkins node is removed.
     * 
     * @param cloud The CPLN cloud configuration
     * @param workloadName The name of the workload to delete
     * @return true if workload was deleted or didn't exist
     */
    public static boolean deleteWorkloadSync(Cloud cloud, String workloadName) {
        if (cloud == null) {
            LOGGER.log(WARNING, "Cloud is null, cannot delete workload {0}", workloadName);
            return false;
        }
        
        try {
            LOGGER.log(INFO, "Deleting CPLN workload {0} synchronously", workloadName);
            
            HttpResponse<String> response = send(request(
                    String.format(Workload.DELETEURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.DELETE, cloud.getApiKey().getPlainText()));
            
            int statusCode = response.statusCode();
            
            if (statusCode == 202 || statusCode == 200 || statusCode == 204) {
                LOGGER.log(INFO, "Successfully deleted CPLN workload {0}", workloadName);
                return true;
            }
            
            if (statusCode == 404) {
                LOGGER.log(INFO, "CPLN workload {0} not found (already deleted)", workloadName);
                return true;
            }
            
            LOGGER.log(WARNING, "Failed to delete CPLN workload {0}: {1} - {2}",
                    new Object[]{workloadName, statusCode, response.body()});
            return false;
            
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error deleting CPLN workload {0}: {1}",
                    new Object[]{workloadName, e.getMessage()});
            return false;
        }
    }

    /**
     * Check if a CPLN workload exists.
     * 
     * @param cloud The CPLN cloud configuration
     * @param workloadName The name of the workload to check
     * @return true if workload exists, false otherwise
     */
    public static boolean workloadExists(Cloud cloud, String workloadName) {
        if (cloud == null || workloadName == null) {
            return false;
        }
        
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.GETURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            return response.statusCode() == 200;
            
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error checking workload existence for {0}: {1}",
                    new Object[]{workloadName, e.getMessage()});
            return false;
        }
    }

    /**
     * Check if a node has busy executors.
     * 
     * @param nodeName The name of the node to check
     * @return true if node has busy executors
     */
    public static boolean hasBusyExecutors(String nodeName) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return false;
        
        hudson.model.Computer computer = jenkins.getComputer(nodeName);
        if (computer == null) return false;
        
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
     * Remove a stale node that has no backing workload.
     * This should be called when a node exists but its workload does not.
     * 
     * @param cloud The CPLN cloud configuration
     * @param nodeName The name of the node to check and potentially remove
     * @return true if node was removed or didn't need removal
     */
    public static boolean removeStaleNode(Cloud cloud, String nodeName) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return false;
        
        hudson.model.Node node = jenkins.getNode(nodeName);
        if (node == null) {
            // Node doesn't exist, nothing to do
            return true;
        }
        
        // Check if workload exists
        if (workloadExists(cloud, nodeName)) {
            // Workload exists, node is not stale
            return false;
        }
        
        // Workload doesn't exist but node does - this is a stale node
        LOGGER.log(WARNING, "Node {0} has no backing workload. Removing stale node.", nodeName);
        
        // Check for busy executors
        if (hasBusyExecutors(nodeName)) {
            LOGGER.log(WARNING, "Cannot remove stale node {0} - has busy executors", nodeName);
            return false;
        }
        
        return removeNodeSync(nodeName);
    }
}

