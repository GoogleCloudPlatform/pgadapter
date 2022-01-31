package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTID extends SimpleNode {
  private String name;

  public ASTID(int id) {
    super(id);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public String getFormattedName() {
    if (name.length() > 0 && (name.charAt(0) == '\'' || name.charAt(0) == '\"')) {
      return name.substring(1, name.length() - 1);
    } else {
      return name.toLowerCase();
    }
  }

  public String toString() {
    return "Identifier: " + name;
  }
}
