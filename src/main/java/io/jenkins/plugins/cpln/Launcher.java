package io.jenkins.plugins.cpln;

import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.slaves.ComputerLauncher;
import hudson.slaves.JNLPLauncher;
import hudson.slaves.SlaveComputer;
import io.jenkins.plugins.cpln.model.Container;
import io.jenkins.plugins.cpln.model.Workload;
import org.kohsuke.stapler.DataBoundConstructor;

import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.logging.Logger;

import static io.jenkins.plugins.cpln.Utils.*;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

public class Launcher extends JNLPLauncher {

    private static final Logger LOGGER = Logger.getLogger(Launcher.class.getName());
    private static final String AGENT_IMAGE = "jenkins/inbound-agent";
    private static final String AGENT_CONTAINER = "inbound-agent";

    private volatile boolean launched = false;

    @DataBoundConstructor
    public Launcher() {
        super();
    }

    @Override
    public boolean isLaunchSupported() {
        return !launched;
    }

    @Override
    @SuppressFBWarnings
    public synchronized void launch(SlaveComputer computer, TaskListener listener) {
        if (!(computer instanceof Computer)) {
            throw new IllegalArgumentException("Invalid Cpln Computer Specification");
        }

        Agent node = ((Computer)computer).getNode();
        if (node == null) {
            throw new IllegalStateException("No node for computer " + computer.getName());
        }
        if (launched) {
            computer.setAcceptingTasks(true);
            return;
        }

        Cloud cloud = ((Agent) computer.getNode()).getCloud();
        try {
            if(Objects.isNull(cloud)){
                return;
            }
            LOGGER.log(INFO, "Attempting to launch workload on cloud {0} with workload {1}...",
                    new Object[]{cloud, computer.getName()});

            if(allowLaunch(cloud, computer)) {
                if(createWorkload(cloud, computer)){
                    computer.setAcceptingTasks(true);
                    launched = true;
                    LOGGER.log(INFO, "Workload agent successfully deployed on cloud {0} with workload {1} - retention {2} min(s)",
                            new Object[]{cloud, computer.getName(), cloud.getRetentionMins()});
                }
            } else {
                LOGGER.log(INFO, "Found workload agent already deployed on cloud {0} with workload {1}...",
                        new Object[]{cloud, computer.getName()});
            }
        } catch (Exception e) {
            LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                    new Object[]{cloud, e});
        }
    }

    public Descriptor<ComputerLauncher> getDescriptor() {
        return new DescriptorImpl();
    }

    @SuppressFBWarnings
    private boolean allowLaunch(Cloud cloud, SlaveComputer computer) throws Exception {
        HttpResponse<String> response = send(request(
                        String.format(Workload.GETURI, cloud.getOrg(), cloud.getGvc(), computer.getName()),
                        SendType.GET, cloud.getApiKey().getPlainText()));
        if ((response.statusCode() >= 300 && response.statusCode() < 400)
            || response.statusCode() >= 500) {
            LOGGER.log(WARNING, "Workload verification failed for {0} with invalid status: {1} - {2}",
                    new Object[]{cloud, response.statusCode(), response.body()});
            return false;
        }
        if (response.statusCode() == 400 || response.statusCode() == 404) {
            LOGGER.log(INFO, "Workload not found for {0}: {1}",
                    new Object[]{cloud, response.statusCode()});
            return true;
        }
        boolean hasAgentImage = false;
        boolean hasAgentContainer = false;
        if (response.statusCode() == 200) {
            Workload workload = readGetWorkload(response.body());
            for(Container container : workload.spec.containers){
                if(AGENT_IMAGE.equals(container.image)) {
                    hasAgentImage = true;
                }
                if(AGENT_CONTAINER.equals(container.name)){
                    hasAgentContainer = true;
                }
            }
            if(!hasAgentImage) {
                LOGGER.log(WARNING, "Workload at {0} should have an agent image: {1}",
                        new Object[]{cloud, AGENT_IMAGE});
            }
            if(!hasAgentContainer){
                LOGGER.log(WARNING, "Workload at {0} should have a container name : {1}",
                        new Object[]{cloud, AGENT_CONTAINER});
            }
        } else {
            LOGGER.log(WARNING, "Unknown response for {0}: {1} - {2}",
                    new Object[]{cloud, response.statusCode(), response.body()});
        }
        return false;
    }

    @SuppressFBWarnings
    private boolean createWorkload(Cloud cloud, SlaveComputer computer) {
        String workloadBody = Utils.resolveWorkloadTemplate(cloud, computer);
        if(Strings.isNullOrEmpty(workloadBody)) {
            return false;
        }
        try {
            HttpResponse<String> response = send(request(
                            String.format(Workload.CREATEURI, cloud.getOrg(), cloud.getGvc()),
                            SendType.POST, workloadBody, cloud.getApiKey().getPlainText()));
            if (response.statusCode() == 201) {
                return true;
            }
            if (response.statusCode() >= 300 || response.statusCode() <= 200) {
                LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                        new Object[]{cloud, response.body()});
                return false;
            }
        } catch (Exception e) {
            LOGGER.log(WARNING, "Workload provisioning failed for {0}: {1}",
                    new Object[]{cloud, e});
            return false;
        }
        return false;
    }

    private static class DescriptorImpl extends Descriptor<ComputerLauncher> {}
}
