package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTColumnElement extends SimpleNode {
  private String name;
  public ASTColumnElement(int id) {
    super(id);
  }

  public ASTColumnElement(Copy p, int id) {
    super(p, id);
  }

  public void setName(String n) {
    name = n;
  }

  public String toString() {
    return "Column Element: " + name;
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
