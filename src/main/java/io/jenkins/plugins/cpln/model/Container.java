package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings
public class Container {
    public String name;
    public String image;
    public List<EnvVar> env;
}
