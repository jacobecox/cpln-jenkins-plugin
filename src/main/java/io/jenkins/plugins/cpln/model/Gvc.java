package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings
public class Gvc {
    public static final String URI = "org/%s/gvc";
    public String id;
    public String name;
    public String description;
    public String version;
    public Spec spec;
}
