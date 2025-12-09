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
import java.util.Objects;
import java.util.logging.Logger;

import static java.util.logging.Level.WARNING;

public class Utils {
    private static final String CPLN_URL = System.getenv("CPLN_ENDPOINT") != null ? System.getenv("CPLN_ENDPOINT") : "https://api.cpln.io";
    private static final String CPLN_OBJECT_TYPE = "application/json";
    public static final String CPLN_AGENT_NAME_PREFIX = ""; // TBD
    private static final String DEFAULT_AGENT_SPEC = "specs/default-agent-spec.json";
    private static final String DEFAULT_AGENT_SPEC_WITH_VOLUMES = "specs/default-agent-spec-with-volumes.json";

    private static final HttpClient httpClient = HttpClient.newHttpClient();
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
            .build();
    }

    public static HttpResponse<String> send(HttpRequest request) throws Exception {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public static CloudItems<Org> readGetOrgs(String response) throws Exception{
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<Gvc> readGetGvcs(String response) throws Exception{
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static Workload readGetWorkload(String response) throws Exception{
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<VolumeSet> readGetVolumeSets(String response) throws Exception{
        return objectMapper.readValue(response, new TypeReference<>() {});
    }

    public static CloudItems<Identity> readGetIdentities(String response) throws Exception{
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
            body = body.replace("<<CPLN-AGENT-DESC>>","Cpln Jenkins Agent Workload");
            body = body.replace("<<CPLN-GVC-NAME>>", cloud.getGvc());
            body = body.replace("<<CPLN-AGENT-CONTAINER-CPU>>", String.format("%d", cloud.getCpu()));
            body = body.replace("<<CPLN-AGENT-CONTAINER-MEMORY>>", String.format("%d", cloud.getMemory()));
            body = body.replace("<<JENKINS-AGENT-NAME>>", cplnAgentName);
            body = body.replace("<<JENKINS-AGENT-IMAGE>>", cloud.getAgentImage());
            body = body.replace("<<JENKINS-AGENT-SECRET>>", computer.getJnlpMac());
            if(withVolumes){
                body = body.replace("<<CPLN-VOLUMESET-NAME>>", cloud.getVolumeSetName());
                body = body.replace("<<CPLN-VOLUMESET-PATH>>", cloud.getVolumeSetPath());
            }
            if(!Strings.isNullOrEmpty(cloud.getIdentity())) {
                body = body.replace("<<CPLN-IDENTITY-LINK>>",
                        "\"identityLink\": \"<<CPLN-IDENTITY>>\",");
                body = body.replace("<<CPLN-IDENTITY>>",
                        String.format("/org/%s/gvc/%s/identity/%s",
                                cloud.getOrg(), cloud.getGvc(), cloud.getIdentity()));
            } else {
                body = body.replace("<<CPLN-IDENTITY-LINK>>","");
            }
            return body.replace("<<JENKINS-CONTROLLER-URL>>", cloud.getJenkinsControllerUrl());
        } catch (Exception e) {
            LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                    new Object[]{cloud, e});
            return null;
        }
    }
}

