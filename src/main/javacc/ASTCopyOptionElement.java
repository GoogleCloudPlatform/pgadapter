package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTCopyOptionElement extends SimpleNode {
  private String name;

  public ASTCopyOptionElement(int id) {
    super(id);
  }

  public ASTCopyOptionElement(Copy p, int id) {
    super(p, id);
  }

  public void setName(String n) {
    name = n;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return "Copy Option Element: " + name;
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
