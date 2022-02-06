package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTFilename extends SimpleNode {
  private String name;

  public ASTFilename(int id) {
    super(id);
  }

  public ASTFilename(Copy p, int id) {
    super(p, id);
  }

  public void setName(String n) {
    name = n;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return "Filename: " + name;
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
