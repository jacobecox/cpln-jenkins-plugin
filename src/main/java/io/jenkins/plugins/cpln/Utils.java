package io.jenkins.plugins.cpln;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import hudson.slaves.SlaveComputer;
import io.jenkins.plugins.cpln.model.*;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.logging.Logger;

import static java.util.logging.Level.WARNING;

/**
 * Utility class for CPLN API interactions and workload management.
 */
public class Utils {
    private static final String CPLN_URL = System.getenv("CPLN_ENDPOINT") != null ? 
            System.getenv("CPLN_ENDPOINT") : "https://api.cpln.io";
    private static final String CPLN_OBJECT_TYPE = "application/json";
    public static final String CPLN_AGENT_NAME_PREFIX = ""; // TBD
    private static final String DEFAULT_AGENT_SPEC = "specs/default-agent-spec.json";
    private static final String DEFAULT_AGENT_SPEC_WITH_VOLUMES = "specs/default-agent-spec-with-volumes.json";
    
    // CPU and Memory minimums - realistic values for Jenkins inbound-agent (Java process)
    // The CPLN platform allows lower values, but the Jenkins agent won't start properly
    public static final int MIN_CPU_MILLICORES = 50;
    public static final int MIN_MEMORY_MEBIBYTES = 128;
    
    // Recommended values - below these, the agent may start but run poorly or OOM
    public static final int RECOMMENDED_CPU_MILLICORES = 200;
    public static final int RECOMMENDED_MEMORY_MEBIBYTES = 256;

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    public static HttpRequest request(String uri, SendType sendType, String key) {
        return request(uri, sendType, null, key);
    }

    public static HttpRequest request(String uri, SendType sendType, String body, String key) {
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        if (sendType == SendType.POST) {
            builder = builder.POST(HttpRequest.BodyPublishers.ofString(body));
            builder = builder.header("content-type", "application/json");
        } else if (sendType == SendType.PUT) {
            builder = builder.PUT(HttpRequest.BodyPublishers.ofString(body));
            builder = builder.header("content-type", "application/json");
        } else if (sendType == SendType.DELETE) {
            builder = builder.DELETE();
        }

        return builder.uri(URI.create(String.format("%s/%s", CPLN_URL, uri)))
            .header("accept", CPLN_OBJECT_TYPE)
            .setHeader("authorization", key)
            .timeout(Duration.ofSeconds(60))
            .build();
    }

    public static HttpResponse<String> send(HttpRequest request) throws Exception {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public static CloudItems<Org> readGetOrgs(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<Gvc> readGetGvcs(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static Workload readGetWorkload(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    /**
     * Read a list of workloads from a CPLN API response.
     */
    public static CloudItems<Workload> readGetWorkloads(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<VolumeSet> readGetVolumeSets(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<Identity> readGetIdentities(String response) throws Exception {
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static String resolveWorkloadTemplate(Cloud cloud, SlaveComputer computer) {
        boolean withVolumes = !Strings.isNullOrEmpty(cloud.getVolumeSetName());
        String cplnAgentSpec = withVolumes ?
                DEFAULT_AGENT_SPEC_WITH_VOLUMES : DEFAULT_AGENT_SPEC;
        try (InputStream is = Utils.class.getResourceAsStream(cplnAgentSpec)) {
            String body = new String(Objects.requireNonNull(is).readAllBytes(), StandardCharsets.UTF_8);
            String cplnAgentName = computer.getName();
            body = body.replace("<<CPLN-AGENT-NAME>>", cplnAgentName);
            body = body.replace("<<CPLN-AGENT-DESC>>", "Cpln Jenkins Agent Workload");
            body = body.replace("<<CPLN-GVC-NAME>>", cloud.getGvc());
            body = body.replace("<<CPLN-AGENT-CONTAINER-CPU>>", String.format("%d", cloud.getCpu()));
            body = body.replace("<<CPLN-AGENT-CONTAINER-MEMORY>>", String.format("%d", cloud.getMemory()));
            body = body.replace("<<JENKINS-AGENT-NAME>>", cplnAgentName);
            body = body.replace("<<JENKINS-AGENT-IMAGE>>", cloud.getAgentImage());
            body = body.replace("<<JENKINS-AGENT-SECRET>>", computer.getJnlpMac());
            if (withVolumes) {
                body = body.replace("<<CPLN-VOLUMESET-NAME>>", cloud.getVolumeSetName());
                body = body.replace("<<CPLN-VOLUMESET-PATH>>", cloud.getVolumeSetPath());
            }
            if (!Strings.isNullOrEmpty(cloud.getIdentity())) {
                body = body.replace("<<CPLN-IDENTITY-LINK>>",
                        "\"identityLink\": \"<<CPLN-IDENTITY>>\",");
                body = body.replace("<<CPLN-IDENTITY>>",
                        String.format("/org/%s/gvc/%s/identity/%s",
                                cloud.getOrg(), cloud.getGvc(), cloud.getIdentity()));
            } else {
                body = body.replace("<<CPLN-IDENTITY-LINK>>", "");
            }
            return body.replace("<<JENKINS-CONTROLLER-URL>>", cloud.getJenkinsControllerUrl());
        } catch (Exception e) {
            LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                    new Object[]{cloud, e});
            return null;
        }
    }

    /**
     * Check if a workload exists in CPLN.
     * 
     * @param cloud The CPLN cloud configuration
     * @param workloadName The name of the workload to check
     * @return true if the workload exists, false otherwise
     */
    public static boolean workloadExists(Cloud cloud, String workloadName) {
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
     * Delete a workload from CPLN.
     * 
     * @param cloud The CPLN cloud configuration
     * @param workloadName The name of the workload to delete
     * @return true if deletion was successful or workload didn't exist, false on error
     */
    public static boolean deleteWorkload(Cloud cloud, String workloadName) {
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.DELETEURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.DELETE, cloud.getApiKey().getPlainText()));
            
            int statusCode = response.statusCode();
            return statusCode == 202 || statusCode == 200 || 
                   statusCode == 204 || statusCode == 404;
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error deleting workload {0}: {1}",
                    new Object[]{workloadName, e.getMessage()});
            return false;
        }
    }

    /**
     * Get workload status from CPLN.
     * 
     * @param cloud The CPLN cloud configuration
     * @param workloadName The name of the workload
     * @return The workload object or null if not found or on error
     */
    public static Workload getWorkload(Cloud cloud, String workloadName) {
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.GETURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            if (response.statusCode() == 200) {
                return readGetWorkload(response.body());
            }
            return null;
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error getting workload {0}: {1}",
                    new Object[]{workloadName, e.getMessage()});
            return null;
        }
    }

    /**
     * List all workloads in a GVC.
     * 
     * @param cloud The CPLN cloud configuration
     * @return CloudItems containing workloads, or null on error
     */
    public static CloudItems<Workload> listWorkloads(Cloud cloud) {
        try {
            HttpResponse<String> response = send(request(
                    String.format(Workload.LISTURI, cloud.getOrg(), cloud.getGvc()),
                    SendType.GET, cloud.getApiKey().getPlainText()));
            
            if (response.statusCode() == 200) {
                return readGetWorkloads(response.body());
            }
            LOGGER.log(WARNING, "Failed to list workloads: {0} - {1}",
                    new Object[]{response.statusCode(), response.body()});
            return null;
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error listing workloads: {0}", e.getMessage());
            return null;
        }
    }

    /**
     * Validate CPU value against minimum.
     * 
     * @param cpu CPU in millicores
     * @return true if valid, false otherwise
     */
    public static boolean isValidCpu(int cpu) {
        return cpu >= MIN_CPU_MILLICORES;
    }

    /**
     * Validate memory value against minimum.
     * 
     * @param memory Memory in mebibytes
     * @return true if valid, false otherwise
     */
    public static boolean isValidMemory(int memory) {
        return memory >= MIN_MEMORY_MEBIBYTES;
    }
}
