package io.jenkins.plugins.cpln;

import static io.jenkins.plugins.cpln.Utils.*;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.Util;
import hudson.model.*;
import hudson.model.labels.LabelAtom;
import hudson.slaves.CloudRetentionStrategy;
import hudson.slaves.NodeProvisioner;
import hudson.util.FormValidation;
import hudson.util.ListBoxModel;
import hudson.util.Secret;
import hudson.util.XStream2;
import io.jenkins.plugins.cpln.model.*;

import java.io.IOException;
import java.io.StringReader;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jenkins.model.Jenkins;
import org.kohsuke.stapler.*;

@SuppressFBWarnings
public class Cloud extends hudson.slaves.Cloud {

    private static final Logger LOGGER = Logger.getLogger(Cloud.class.getName());

    private static final String LIST_CHOOSE = "-- Choose one --";
    private static final String LIST_NO_ITEM = "no-item-present";

    private String org;

    private String gvc;

    private String agentWorkload;

    private String labels;

    private boolean useUniqueAgents;

    private boolean allowJobsWithoutLabels;

    private int executors;

    private int cpu;

    private int memory;

    private int retentionMins;

    private String agentImage;

    private String volumeSetName;

    private String volumeSetPath;

    private String identity;

    private String jenkinsControllerUrl;

    private Secret apiKey;

    private static final Pattern labelPattern = Pattern.compile("|(([a-z][-a-z0-9]+)( [a-z][-a-z0-9]+)*)");
    private static final Pattern workloadPattern = Pattern.compile("[a-z][-a-z0-9]+");

    @DataBoundConstructor
    public Cloud(String name, String org,
                 String gvc,
                 String agentWorkload,
                 String labels,
                 boolean useUniqueAgents,
                 boolean allowJobsWithoutLabels,
                 int executors,
                 int cpu,
                 int memory,
                 int retentionMins,
                 String agentImage,
                 String volumeSetName,
                 String volumeSetPath,
                 String identity,
                 String jenkinsControllerUrl,
                 Secret apiKey) {
        super(name);
        this.org = org;
        this.gvc = gvc;
        this.agentWorkload = agentWorkload;
        this.labels = labels;
        this.useUniqueAgents = useUniqueAgents;
        this.allowJobsWithoutLabels = allowJobsWithoutLabels;
        this.executors = executors;
        this.cpu = cpu;
        this.memory = memory;
        this.retentionMins = retentionMins;
        this.agentImage = agentImage;
        this.volumeSetName = volumeSetName;
        this.volumeSetPath = volumeSetPath;
        this.identity = identity;
        this.jenkinsControllerUrl = jenkinsControllerUrl;
        this.apiKey = apiKey;
    }

    public Cloud(@NonNull String name, @NonNull Cloud source) {
        super(name);
        XStream2 xs = new XStream2();
        xs.omitField(hudson.slaves.Cloud.class, "name");
        xs.unmarshal(XStream2.getDefaultDriver().createReader(new StringReader(xs.toXML(source))), this);
    }

    public String getOrg() {
        return org;
    }

    @DataBoundSetter
    public void setOrg(@NonNull String org) {
        this.org = Util.fixEmpty(org);
    }

    public String getGvc() {
        return gvc;
    }

    @DataBoundSetter
    public void setGvc(@NonNull String gvc) {
        this.gvc = Util.fixEmpty(gvc);
    }

    public String getAgentWorkload() {
        return agentWorkload;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setAgentWorkload(@NonNull String agentWorkload) {
        this.agentWorkload = agentWorkload;
    }

    public String getLabels() {
        return labels;
    }

    public List<LabelAtom> getLabelAtoms() {
        return Arrays.stream(labels.split(" ")).map(LabelAtom::new).collect(Collectors.toList());
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setLabels(@NonNull String labels) {
        this.labels = labels;
    }

    public boolean getUseUniqueAgents() {
        return useUniqueAgents;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setUseUniqueAgents(boolean useUniqueAgents) {
        this.useUniqueAgents = useUniqueAgents;
    }

    public boolean getAllowJobsWithoutLabels() {
        return allowJobsWithoutLabels;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setAllowJobsWithoutLabels(boolean allowJobsWithoutLabels) {
        this.allowJobsWithoutLabels = allowJobsWithoutLabels;
    }

    public int getExecutors() {
        return executors;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setExecutors(int executors) {
        this.executors = executors;
    }

    public int getCpu() {
        return cpu;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public int getMemory() {
        return memory;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getRetentionMins() {
        return retentionMins;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setRetentionMins(int retentionMins) {
        this.retentionMins = retentionMins;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setAgentImage(@NonNull String agentImage) {
        this.agentImage = Util.fixEmpty(agentImage);
    }

    public String getAgentImage() {
        return agentImage;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setVolumeSetName(@NonNull String volumeSetName) {
        this.volumeSetName = volumeSetName;
    }

    public String getVolumeSetName() {
        return volumeSetName;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setVolumeSetPath(@NonNull String volumeSetPath) {
        this.volumeSetPath = volumeSetPath;
    }

    public String getVolumeSetPath() {
        return volumeSetPath;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setIdentity(@NonNull String identity) {
        this.identity = identity;
    }

    @SuppressWarnings("unused")
    public String getIdentity() {
        return identity;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setJenkinsControllerUrl(@NonNull String jenkinsControllerUrl) {
        this.jenkinsControllerUrl = Util.fixEmpty(jenkinsControllerUrl);
    }

    public String getJenkinsControllerUrl() {
        return jenkinsControllerUrl;
    }

    public Secret getApiKey() {
        return apiKey;
    }

    @DataBoundSetter
    @SuppressWarnings("unused")
    public void setApiKey(@NonNull Secret apiKey) {
        this.apiKey = apiKey;
    }

    public boolean canProvision(@NonNull hudson.slaves.Cloud.CloudState state) {
        return true;
    }

    @Override
    public Collection<NodeProvisioner.PlannedNode> provision(
            @NonNull final hudson.slaves.Cloud.CloudState state, final int excessWorkload) {

        // configuring the cloud with no executors prevents new jobs from
        // being assigned to agents of this cloud. Updating the executor number to > 0
        // results in any already waiting job being able to run on agents of this cloud again.
        if(getExecutors() == 0){
            return Collections.emptyList();
        }

        String label;
        Stream<Node> nodes;
        if(Objects.nonNull(state.getLabel())) {
            label = state.getLabel().getName();
            if(!state.getLabel().matches(getLabelAtoms())){
                LOGGER.log(WARNING, "Label {0} not supported by {1}. Use one of '{2}'",
                        new Object[]{label, this, getLabels()});
                return Collections.emptyList();
            }
            nodes = state.getLabel().getNodes().stream();
        } else {
            label = "";
            nodes = Jenkins.get().getNodes().stream();
        }

        String labelSuffix = label.isEmpty() ? "" : String.format("-%s", label);
        String agentNameBase = String.format("%s%s%s", CPLN_AGENT_NAME_PREFIX, getAgentWorkload(), labelSuffix);

        int realExcessWorkload = 1;
        Set<Node> nodeSet = new LinkedHashSet<>();
        if(!getUseUniqueAgents()) {
            final String agentNameUnlabeled = agentNameBase;
            nodes = nodes.filter(Agent.class::isInstance)
                    .filter(Node::isAcceptingTasks)
                    .filter(n -> n.getNodeName().equals(agentNameUnlabeled));
            nodeSet = nodes.collect(Collectors.toSet());
        } else {
            realExcessWorkload = excessWorkload;
        }
        if(!getUseUniqueAgents() && !nodeSet.isEmpty()) {
            String labelInfo = label.isEmpty() ? "no label" : String.format("label: %s", label);
            LOGGER.log(INFO, "Agent found for {0}: {1} for tasks with {2}",
                    new Object[]{this, nodeSet.iterator().next().getNodeName(), labelInfo});
            return Collections.emptyList();
        }

        // if the cloud has labels, it can choose to take or not to take unlabeled jobs
        if(label.isEmpty() && !Strings.isNullOrEmpty(getLabels()) && !getAllowJobsWithoutLabels()){
            return Collections.emptyList();
        }

        try {
            Set<NodeProvisioner.PlannedNode> plannedNodes = new LinkedHashSet<>();
            while(realExcessWorkload-- > 0) {
                String agentName = getAgentName(agentNameBase);
                Agent agent = new Agent(agentName, "", new Launcher());
                agent.setCloud(this);
                agent.setLabelString(label);
                agent.setNumExecutors(getExecutors());
                agent.setRetentionStrategy(new CloudRetentionStrategy(getRetentionMins()));
                NodeProvisioner.PlannedNode node = new NodeProvisioner.PlannedNode(
                        name, CompletableFuture.completedFuture(agent), getExecutors());
                plannedNodes.add(node);
            }
            return plannedNodes;
        } catch (Descriptor.FormException | IOException e) {
            LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                    new Object[]{this, e});
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unused")
    public String toString() {
        return String.format("%s: %s/%s/%s",
                getDisplayName(), getOrg(), getGvc(), getAgentWorkload());
    }

    private String getAgentName(String agentNameBase) {
        if(getUseUniqueAgents()) {
            String uniqueSuffix = String.format("-%s",
                    UUID.randomUUID().toString().substring(0, 8));
            return String.format("%s%s", agentNameBase, uniqueSuffix);
        }
        return agentNameBase;
    }

    @Extension
    @SuppressWarnings("unused")
    public static class DescriptorImpl extends Descriptor<hudson.slaves.Cloud> {

        private final Map<String, Map<String, List<String>>> orgGvcLocations = new HashMap<>();

        @DataBoundConstructor
        public DescriptorImpl() {
            super(Cloud.class);
            load();
        }

        @Override
        @NonNull
        public String getDisplayName() {
            return "Control Plane";
        }

        public ListBoxModel doFillOrgItems(@AncestorInPath ItemGroup owner,
                                            @QueryParameter Secret apiKey) {
            if(Objects.isNull(apiKey) || Strings.isNullOrEmpty(apiKey.getPlainText())) {
                return new ListBoxModel();
            }

            HttpResponse<String> response = null;
            try {
                response = send(request(String.format("%s", Org.URI), SendType.GET, apiKey.getPlainText()));
                if (response.statusCode() >= 300) {
                    LOGGER.log(WARNING, "Unexpected response on Org List: " +
                            "{0} - {1}", new Object[]{response.statusCode(), response.body()});
                    return new ListBoxModel();
                }
                CloudItems<Org> cplnItems = readGetOrgs(response.body());
                ListBoxModel items = new ListBoxModel();
                items.add(LIST_CHOOSE, LIST_NO_ITEM);
                for (Org org : cplnItems.items) {
                    items.add(org.name, org.name);
                }
                return items;
            } catch (Exception e) {
                LOGGER.log(WARNING, "Unexpected response on Org List: " +
                        "{0} - {1} - {2}", new Object[]{
                                Objects.requireNonNull(response).statusCode(),
                                response.body(), e});
                return new ListBoxModel();
            }
        }

        public ListBoxModel doFillGvcItems(@AncestorInPath ItemGroup owner,
                                            @QueryParameter String org,
                                            @QueryParameter Secret apiKey) {
            if(Strings.isNullOrEmpty(org)) {
                return new ListBoxModel();
            }

            HttpResponse<String> response = null;
             try {
                 response = send(request(
                         String.format(Gvc.URI, org), SendType.GET, apiKey.getPlainText()));
                 Map<String, List<String>> gvcLocations = orgGvcLocations.get(org);
                 if (response.statusCode() >= 300) {
                     if (!Objects.isNull(gvcLocations)) {
                         gvcLocations.clear();
                     }
                     LOGGER.log(WARNING, "Unexpected response on Gvc List: " +
                             "{0} - {1}", new Object[]{response.statusCode(), response.body()});
                     return new ListBoxModel();
                 }
                 CloudItems<Gvc> cplnItems = readGetGvcs(response.body());
                 ListBoxModel items = new ListBoxModel();
                 orgGvcLocations.put(org, gvcLocations = new HashMap<>());
                 items.add("-- Choose one", "no-gvc-present");
                 for (Gvc gvc : cplnItems.items) {
                     List<String> locationValues = new LinkedList<>();
                     for (String location : gvc.spec.staticPlacement.locationLinks) {
                         String locationValue = Arrays.stream(
                                         location.split("/"))
                                 .reduce((first, second) -> second)
                                 .orElse("");
                         locationValues.add(locationValue);
                     }
                     gvcLocations.put(gvc.name, locationValues);
                     items.add(String.format("%s @ %s", gvc.name, locationValues), gvc.name);
                 }
                 return items;
             } catch (Exception e) {
                 LOGGER.log(WARNING, "Unexpected response on Gvc List: " +
                         "{0} - {1} - {2}", new Object[]{
                                 Objects.requireNonNull(response).statusCode(), response.body(), e});
                 return new ListBoxModel();
             }
        }

        public ListBoxModel doFillVolumeSetNameItems(@AncestorInPath ItemGroup owner,
                                           @QueryParameter String org,
                                           @QueryParameter String gvc,
                                           @QueryParameter Secret apiKey) {
            if(Strings.isNullOrEmpty(org) || Strings.isNullOrEmpty(gvc)) {
                return new ListBoxModel();
            }

            HttpResponse<String> response = null;
            try {
                response = send(request(String.format(VolumeSet.URI, org, gvc), SendType.GET, apiKey.getPlainText()));
                if (response.statusCode() != 200) {
                    LOGGER.log(WARNING, "Unexpected response on Volume Set List: " +
                            "{0} - {1}", new Object[]{response.statusCode(), response.body()});
                    return new ListBoxModel();
                }
                CloudItems<VolumeSet> cplnItems = readGetVolumeSets(response.body());
                ListBoxModel items = new ListBoxModel();
                items.add("-- Choose one", "no-volumesets-present");
                items.add("", "");
                for (VolumeSet volumeSet : cplnItems.items) {
                    String name = volumeSet.name;
                    String description = volumeSet.description;
                    String suffix = Strings.isNullOrEmpty(description) ? "" : String.format(" (%s)", description);
                    items.add(String.format("%s%s", name, suffix), name);
                }
                return items;
            } catch (Exception e) {
                LOGGER.log(WARNING, "Unexpected response on VolumeSet List: " +
                        "{0} - {1} - {2}", new Object[]{
                                Objects.requireNonNull(response).statusCode(), response.body(), e});
                return new ListBoxModel();
            }
        }

        public ListBoxModel doFillIdentityItems(@AncestorInPath ItemGroup owner,
                                                     @QueryParameter String org,
                                                     @QueryParameter String gvc,
                                                     @QueryParameter Secret apiKey) {
            if(Strings.isNullOrEmpty(org) || Strings.isNullOrEmpty(gvc)) {
                return new ListBoxModel();
            }

            HttpResponse<String> response = null;
            try {
                response = send(request(String.format(Identity.URI, org, gvc), SendType.GET, apiKey.getPlainText()));
                if (response.statusCode() != 200) {
                    LOGGER.log(WARNING, "Unexpected response on Identity List: " +
                            "{0} - {1}", new Object[]{response.statusCode(), response.body()});
                    return new ListBoxModel();
                }
                CloudItems<Identity> cplnItems = readGetIdentities(response.body());
                ListBoxModel items = new ListBoxModel();
                items.add("-- Choose one", "no-identities-present");
                items.add("", "");
                for (Identity identity : cplnItems.items) {
                    String name = identity.name;
                    String description = identity.description;
                    String suffix = Strings.isNullOrEmpty(description) ? "" : String.format(" (%s)", description);
                    items.add(String.format("%s%s", name, suffix), name);
                }
                return items;
            } catch (Exception e) {
                LOGGER.log(WARNING, "Unexpected response on Identity List: " +
                        "{0} - {1} - {2}", new Object[]{
                        Objects.requireNonNull(response).statusCode(), response.body(), e});
                return new ListBoxModel();
            }
        }

        public FormValidation doCheckGvc(@QueryParameter String org, @QueryParameter String gvc) {
            if(orgGvcLocations.isEmpty()) {
                return FormValidation.ok();
            }
            Map<String, List<String>> orgMap = orgGvcLocations.get(org);
            if(orgMap == null || orgMap.isEmpty()) {
                return FormValidation.ok();
            }
            List<String> gvcLocations = orgMap.get(gvc);
            if(gvcLocations == null || gvcLocations.isEmpty()){
                return FormValidation.ok();
            }
            if(gvcLocations.size() == 1){
                return FormValidation.ok();
            }
            return FormValidation.error(
                    String.format("%s/%s must have only one location configured, found %d",
                            org, gvc, gvcLocations.size()));
        }

        public FormValidation doCheckAgentWorkload(@QueryParameter String agentWorkload) {
            if(!workloadPattern.matcher(agentWorkload).matches()){
                return FormValidation
                        .error(String.format("Agent Workload must be of form %s, " +
                                "e.g. myjenkins-east-coast-tester", workloadPattern.pattern()));
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckLabels(@QueryParameter String labels) {
            if(!labelPattern.matcher(labels).matches()){
                return FormValidation
                        .error(String.format("Labels must be of form %s, " +
                                "e.g. 'label1' or 'label1 label2 linux-high-load' without the quotes",
                                    labelPattern.pattern()));
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckExecutors(@QueryParameter int executors) {
            if(executors < 0){
                return FormValidation
                        .error(String.format("The # of executors must be a non-negative integer, found %d", executors));
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckCpu(@QueryParameter int cpu) {
            if(cpu <= 0){
                return FormValidation
                        .error(String.format("The CPU millicores must be a positive integer, found %d", cpu));
            }
            return FormValidation.ok();
        }

        public FormValidation doChecMemory(@QueryParameter int memory) {
            if(memory <= 0){
                return FormValidation
                        .error(String.format("The Memory mebibytes must be a positive integer, found %d", memory));
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckRetentionMins(@QueryParameter int retentionMins) {
            if(retentionMins < 1){
                return FormValidation
                        .error(String.format("The Idle Agent Retention Time must be >= 1 minute, found %d",
                                retentionMins));
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckAgentImage(@QueryParameter String agentImage) {
            if(Strings.isNullOrEmpty(agentImage)){
                return FormValidation
                        .error("The Jenkins Agent Image cannot be empty");
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckVolumeSetPath(@QueryParameter String volumeSetName, @QueryParameter String volumeSetPath) {
            if((Strings.isNullOrEmpty(volumeSetName) && !Strings.isNullOrEmpty(volumeSetPath))
            || (!Strings.isNullOrEmpty(volumeSetName) && Strings.isNullOrEmpty(volumeSetPath))){
                return FormValidation
                        .error("The Volume Set Path cannot be empty if the Volume Set Name is not empty and vice versa");
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckJenkinsControllerUrl(@QueryParameter String jenkinsControllerUrl) {
            if(Strings.isNullOrEmpty(jenkinsControllerUrl)){
                return FormValidation
                        .error("The Jenkins Controller Url cannot be empty");
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckApiKey(@QueryParameter String apiKey) {
            if(Strings.isNullOrEmpty(apiKey)){
                return FormValidation
                        .error("The Control Plane Api Key cannot be empty");
            }
            return FormValidation.ok();
        }
    }
}
