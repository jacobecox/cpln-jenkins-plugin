package io.jenkins.plugins.cpln;

import hudson.slaves.AbstractCloudComputer;

public class Computer extends AbstractCloudComputer<Agent> {

    public Computer(Agent slave) {
        super(slave);
    }
}
