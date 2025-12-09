package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings
public class Spec {
    public StaticPlacement staticPlacement;
    public List<Container> containers;
}
