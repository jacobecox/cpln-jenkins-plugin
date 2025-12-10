package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.AsyncPeriodicWork;
import hudson.model.Node;
import hudson.model.TaskListener;
import io.jenkins.plugins.cpln.model.CloudItems;
import io.jenkins.plugins.cpln.model.Workload;
import jenkins.model.Jenkins;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static io.jenkins.plugins.cpln.Utils.*;
import static java.util.logging.Level.*;

/**
 * WorkloadReconciler performs periodic cleanup of CPLN workloads.
 * 
 * This reconciler is completely independent from Jenkins agent/remoting lifecycle
 * and ensures workloads are cleaned up regardless of:
 * - Handshake failures
 * - ClosedChannelException
 * - Missing X-Remoting-Capability headers
 * - OOM kills
 * - Fast exits
 * - Abnormal websocket terminations
 * - Jenkins controller restarts
 * 
 * The reconciler:
 * 1. Lists all CPLN workloads that match the Jenkins agent pattern
 * 2. Compares against currently registered Jenkins nodes
 * 3. Checks workload health/status via CPLN API
 * 4. Deletes orphan workloads or workloads that have terminated/crashed
 */
@Extension
@SuppressFBWarnings
public class WorkloadReconciler extends AsyncPeriodicWork {

    private static final Logger LOGGER = Logger.getLogger(WorkloadReconciler.class.getName());
    
    // Reconciliation interval in milliseconds (default: 60 seconds)
    private static final long RECONCILE_INTERVAL_MS = Long.parseLong(
            System.getProperty("cpln.reconcile.interval.ms", "60000"));
    
    // Grace period before deleting orphan workloads (default: 2 minutes)
    // This prevents deleting workloads that are still starting up
    private static final long ORPHAN_GRACE_PERIOD_MS = Long.parseLong(
            System.getProperty("cpln.orphan.grace.period.ms", "120000"));
    
    // Track when workloads were first seen as orphans (for grace period)
    private final Map<String, Instant> orphanFirstSeen = new ConcurrentHashMap<>();
    
    // Track workloads that were provisioned (to detect crashes)
    private static final Map<String, WorkloadInfo> trackedWorkloads = new ConcurrentHashMap<>();

    public WorkloadReconciler() {
        super("CPLN Workload Reconciler");
    }

    @Override
    public long getRecurrencePeriod() {
        return RECONCILE_INTERVAL_MS;
    }

    @Override
    protected void execute(TaskListener listener) throws IOException, InterruptedException {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) {
            LOGGER.log(FINE, "Jenkins instance not available, skipping reconciliation");
            return;
        }

        LOGGER.log(FINE, "Starting CPLN workload reconciliation...");
        
        // Get all CPLN clouds
        for (hudson.slaves.Cloud cloud : jenkins.clouds) {
            if (cloud instanceof Cloud) {
                reconcileCloud((Cloud) cloud, jenkins);
            }
        }
        
        // Clean up tracked workloads for deleted nodes
        cleanupTrackedWorkloads(jenkins);
        
        LOGGER.log(FINE, "CPLN workload reconciliation completed");
    }

    /**
     * Reconcile workloads for a specific CPLN cloud.
     */
    private void reconcileCloud(Cloud cloud, Jenkins jenkins) {
        try {
            // List all workloads in the GVC
            CloudItems<Workload> workloads = listWorkloads(cloud);
            if (workloads == null || workloads.items == null) {
                LOGGER.log(WARNING, "Failed to list workloads for cloud {0}", cloud);
                return;
            }

            // Get currently registered Jenkins nodes
            Set<String> registeredNodes = new HashSet<>();
            for (Node node : jenkins.getNodes()) {
                if (node instanceof Agent) {
                    Agent agent = (Agent) node;
                    if (agent.getCloud() != null && 
                        cloud.name.equals(agent.getCloud().name)) {
                        registeredNodes.add(node.getNodeName());
                    }
                }
            }

            // Check each workload
            for (Workload workload : workloads.items) {
                if (workload.name == null) continue;
                
                // Skip workloads that don't look like Jenkins agents
                if (!isJenkinsAgentWorkload(workload, cloud)) {
                    continue;
                }

                boolean shouldDelete = false;
                String reason = null;

                // Check 1: Is the workload registered in Jenkins?
                if (!registeredNodes.contains(workload.name)) {
                    // Check grace period
                    Instant firstSeen = orphanFirstSeen.computeIfAbsent(
                            workload.name, k -> Instant.now());
                    
                    if (Duration.between(firstSeen, Instant.now()).toMillis() > ORPHAN_GRACE_PERIOD_MS) {
                        shouldDelete = true;
                        reason = "orphan workload (no matching Jenkins node)";
                    } else {
                        LOGGER.log(FINE, "Workload {0} is orphan but within grace period", workload.name);
                    }
                } else {
                    // Remove from orphan tracking if it's now registered
                    orphanFirstSeen.remove(workload.name);
                }

                // Check 2: Has the workload terminated/crashed based on CPLN status?
                if (!shouldDelete) {
                    WorkloadHealthStatus healthStatus = checkWorkloadHealth(cloud, workload.name);
                    if (healthStatus == WorkloadHealthStatus.TERMINATED || 
                        healthStatus == WorkloadHealthStatus.CRASHED ||
                        healthStatus == WorkloadHealthStatus.OOM_KILLED) {
                        shouldDelete = true;
                        reason = "workload " + healthStatus.name().toLowerCase().replace("_", " ");
                    }
                }

                // Check 3: Is this a tracked workload that's no longer healthy?
                if (!shouldDelete) {
                    WorkloadInfo info = trackedWorkloads.get(workload.name);
                    if (info != null && info.cloudName.equals(cloud.name)) {
                        // Check if workload has been running too long without activity
                        // This catches cases where the agent connected but then died
                        if (info.maxRuntime != null && 
                            Duration.between(info.provisionedAt, Instant.now()).compareTo(info.maxRuntime) > 0) {
                            shouldDelete = true;
                            reason = "exceeded maximum runtime";
                        }
                    }
                }

                if (shouldDelete) {
                    // CRITICAL: Check if agent has busy executors before deleting
                    if (hasBusyExecutors(jenkins, workload.name)) {
                        LOGGER.log(INFO, "Skipping deletion of workload {0} - agent has busy executors (running builds)",
                                workload.name);
                        continue;
                    }
                    
                    // Check if there are queued builds for this agent
                    if (hasQueuedBuilds(jenkins, workload.name)) {
                        LOGGER.log(INFO, "Skipping deletion of workload {0} - there are builds queued for this agent",
                                workload.name);
                        continue;
                    }
                    
                    LOGGER.log(INFO, "Cleaning up workload {0} on cloud {1}: {2}",
                            new Object[]{workload.name, cloud, reason});
                    
                    // CRITICAL: Use centralized cleanup which ensures correct order:
                    // 1. Remove Jenkins node FIRST (prevents scheduling on stale node)
                    // 2. Delete CPLN workload SECOND
                    CplnCleanup.cleanupNodeAndWorkload(cloud, workload.name, reason);
                    
                    orphanFirstSeen.remove(workload.name);
                    trackedWorkloads.remove(workload.name);
                }
            }

            // Clean up orphan tracking for workloads that no longer exist
            Set<String> existingWorkloads = new HashSet<>();
            for (Workload w : workloads.items) {
                if (w.name != null) existingWorkloads.add(w.name);
            }
            orphanFirstSeen.keySet().removeIf(name -> !existingWorkloads.contains(name));

        } catch (Exception e) {
            LOGGER.log(WARNING, "Error reconciling cloud {0}: {1}",
                    new Object[]{cloud, e.getMessage()});
        }
    }

    /**
     * Check if a computer has any busy executors (running builds).
     */
    private boolean hasBusyExecutors(Jenkins jenkins, String computerName) {
        hudson.model.Computer computer = jenkins.getComputer(computerName);
        if (computer == null) return false;
        
        for (hudson.model.Executor executor : computer.getExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        
        for (hudson.model.Executor executor : computer.getOneOffExecutors()) {
            if (executor.isBusy()) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if there are queued builds that could run on this computer.
     */
    private boolean hasQueuedBuilds(Jenkins jenkins, String computerName) {
        hudson.model.Computer computer = jenkins.getComputer(computerName);
        if (computer == null) return false;
        
        hudson.model.Node node = computer.getNode();
        if (node == null) return false;
        
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
     * List all workloads in a cloud's GVC.
     */
    private CloudItems<Workload> listWorkloads(Cloud cloud) {
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.LISTURI, cloud.getOrg(), cloud.getGvc()),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            if (response.statusCode() == 200) {
                return readGetWorkloads(response.body());
            } else {
                LOGGER.log(WARNING, "Failed to list workloads: {0} - {1}",
                        new Object[]{response.statusCode(), response.body()});
                return null;
            }
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error listing workloads: {0}", e.getMessage());
            return null;
        }
    }

    /**
     * Check if a workload appears to be a Jenkins agent workload.
     */
    private boolean isJenkinsAgentWorkload(Workload workload, Cloud cloud) {
        if (workload.description != null && 
            workload.description.contains("Cpln Jenkins Agent")) {
            return true;
        }
        
        // Check if it matches the cloud's agent workload pattern
        String agentWorkloadPrefix = cloud.getAgentWorkload();
        if (agentWorkloadPrefix != null && workload.name != null) {
            return workload.name.startsWith(agentWorkloadPrefix) ||
                   workload.name.equals(agentWorkloadPrefix);
        }
        
        return false;
    }

    /**
     * Check the health status of a workload via CPLN API.
     */
    private WorkloadHealthStatus checkWorkloadHealth(Cloud cloud, String workloadName) {
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.GETURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            if (response.statusCode() == 404) {
                return WorkloadHealthStatus.NOT_FOUND;
            }
            
            if (response.statusCode() != 200) {
                return WorkloadHealthStatus.UNKNOWN;
            }

            Workload workload = readGetWorkload(response.body());
            return evaluateWorkloadStatus(workload);
            
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error checking workload health for {0}: {1}",
                    new Object[]{workloadName, e.getMessage()});
            return WorkloadHealthStatus.UNKNOWN;
        }
    }

    /**
     * Evaluate the status of a workload from its CPLN representation.
     */
    private WorkloadHealthStatus evaluateWorkloadStatus(Workload workload) {
        if (workload.status == null) {
            return WorkloadHealthStatus.UNKNOWN;
        }

        // Check for terminated/crashed status
        if (workload.status.health != null) {
            String health = workload.status.health.toLowerCase();
            if (health.contains("terminated") || health.contains("stopped")) {
                return WorkloadHealthStatus.TERMINATED;
            }
            if (health.contains("crash") || health.contains("error") || health.contains("failed")) {
                return WorkloadHealthStatus.CRASHED;
            }
            if (health.contains("oom") || health.contains("outofmemory") || health.contains("killed")) {
                return WorkloadHealthStatus.OOM_KILLED;
            }
        }

        // Check replica status
        if (workload.status.readyReplicas != null && workload.status.expectedReplicas != null) {
            if (workload.status.readyReplicas == 0 && workload.status.expectedReplicas > 0) {
                // No ready replicas but expecting some - might be crashed/starting
                // Check if it's been in this state for too long
                return WorkloadHealthStatus.UNHEALTHY;
            }
        }

        // If explicitly not active
        if (!workload.status.active) {
            return WorkloadHealthStatus.TERMINATED;
        }

        return WorkloadHealthStatus.HEALTHY;
    }

    /**
     * Delete a workload via CPLN API.
     * IMPORTANT: This method uses CplnCleanup to ensure correct order:
     * 1. Remove Jenkins node FIRST (prevents scheduling on stale node)
     * 2. Delete CPLN workload SECOND
     */
    public static boolean deleteWorkload(Cloud cloud, String workloadName) {
        // Use centralized cleanup which handles the correct order
        return CplnCleanup.cleanupNodeAndWorkload(cloud, workloadName, "reconciler cleanup");
    }

    /**
     * Track a workload for monitoring.
     */
    public static void trackWorkload(String workloadName, String cloudName, Duration maxRuntime) {
        trackedWorkloads.put(workloadName, new WorkloadInfo(workloadName, cloudName, Instant.now(), maxRuntime));
        LOGGER.log(FINE, "Tracking workload {0} for cloud {1}", new Object[]{workloadName, cloudName});
    }

    /**
     * Stop tracking a workload.
     */
    public static void untrackWorkload(String workloadName) {
        trackedWorkloads.remove(workloadName);
        LOGGER.log(FINE, "Untracked workload {0}", workloadName);
    }

    /**
     * Clean up tracked workloads for nodes that no longer exist.
     */
    private void cleanupTrackedWorkloads(Jenkins jenkins) {
        Set<String> currentNodes = new HashSet<>();
        for (Node node : jenkins.getNodes()) {
            currentNodes.add(node.getNodeName());
        }
        
        trackedWorkloads.entrySet().removeIf(entry -> {
            if (!currentNodes.contains(entry.getKey())) {
                LOGGER.log(FINE, "Removing tracked workload {0} - node no longer exists", 
                        entry.getKey());
                return true;
            }
            return false;
        });
    }

    /**
     * Force immediate reconciliation for a specific cloud.
     */
    public static void forceReconcile(Cloud cloud) {
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return;
        
        WorkloadReconciler reconciler = jenkins.getExtensionList(WorkloadReconciler.class).get(0);
        if (reconciler != null) {
            try {
                reconciler.reconcileCloud(cloud, jenkins);
            } catch (Exception e) {
                LOGGER.log(WARNING, "Force reconcile failed for cloud {0}: {1}",
                        new Object[]{cloud, e.getMessage()});
            }
        }
    }

    /**
     * Workload health status enum.
     */
    public enum WorkloadHealthStatus {
        HEALTHY,
        UNHEALTHY,
        TERMINATED,
        CRASHED,
        OOM_KILLED,
        NOT_FOUND,
        UNKNOWN
    }

    /**
     * Information about a tracked workload.
     */
    private static class WorkloadInfo {
        final String workloadName;
        final String cloudName;
        final Instant provisionedAt;
        final Duration maxRuntime;

        WorkloadInfo(String workloadName, String cloudName, Instant provisionedAt, Duration maxRuntime) {
            this.workloadName = workloadName;
            this.cloudName = cloudName;
            this.provisionedAt = provisionedAt;
            this.maxRuntime = maxRuntime;
        }
    }
}

