package com.google.cloud.spanner.pgadapter.parsers;

public class ASTID extends SimpleNode {
  private String name;

  public ASTID(int id) {
    super(id);
  }

  public void setName(String n) {
    name = n;
  }

  public String toString() {
    return "Identifier: " + name;
  }

}
