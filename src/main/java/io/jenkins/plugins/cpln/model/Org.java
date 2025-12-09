package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings
public class Org {
    public static final String URI = "org";
    public String id;
    public String name;
    public String description;
    public String version;
    public Status status;
}
