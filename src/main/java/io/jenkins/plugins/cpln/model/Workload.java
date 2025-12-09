package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings
public class Workload {
    public static final String GETURI = "org/%s/gvc/%s/workload/%s";
    public static final String CREATEURI = "org/%s/gvc/%s/workload";
    public static final String DELETEURI = GETURI;
    public String id;
    public String name;
    public String description;
    public String version;
    public Spec spec;
    public Status status;
}
