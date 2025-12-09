package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.slaves.AbstractCloudSlave;
import hudson.slaves.ComputerLauncher;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.logging.Logger;

import io.jenkins.plugins.cpln.model.Workload;
import org.kohsuke.stapler.DataBoundConstructor;

import static io.jenkins.plugins.cpln.Utils.request;
import static io.jenkins.plugins.cpln.Utils.send;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

public class Agent extends AbstractCloudSlave {

    private static final long serialVersionUID = 5165504654221829569L;

    private static final Logger LOGGER = Logger.getLogger(Agent.class.getName());

    private transient Cloud cloud;

    public Cloud getCloud() {
        return cloud;
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    @DataBoundConstructor
    public Agent(@NonNull String name, String remoteFS, ComputerLauncher launcher)
            throws Descriptor.FormException, IOException {
        super(name, remoteFS, launcher);
    }

    @Override
    public Computer createComputer() {
        return new Computer(this);
    }

    @Override
    protected void _terminate(TaskListener listener) {
        try {
            LOGGER.log(INFO, "Unprovisioning idle agent in cloud {0} with workload {1}...",
                    new Object[]{getCloud(), getNodeName()});
            HttpResponse<String> response = send(request(
                    String.format(Workload.DELETEURI, getCloud().getOrg(), getCloud().getGvc(), getNodeName()),
                    SendType.DELETE, cloud.getApiKey().getPlainText()));
            if ((response.statusCode() == 202)) { // Accepted
                LOGGER.log(INFO, "Successfully unprovisioned idle agent in cloud {0} with workload {1}...",
                        new Object[]{getCloud(), getNodeName()});
                return;
            }
            LOGGER.log(WARNING, "Unprovisioning failed for idle agent in cloud " +
                            "{0} with workload {1}: {2} - {3}",
                    new Object[]{getCloud(), getNodeName(), response.statusCode(), response.body()});
        } catch (Exception e) {
            LOGGER.log(WARNING, "Unprovisioning failed for idle agent in cloud {0} with workload {1}: {2}",
                    new Object[]{getCloud(), getNodeName(), e});
        }
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
