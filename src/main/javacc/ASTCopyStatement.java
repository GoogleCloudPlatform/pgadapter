package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTCopyStatement extends SimpleNode {
  private String name;
  public ASTCopyStatement(int id) {
    super(id);
  }

  public ASTCopyStatement(Copy p, int id) {
    super(p, id);
  }

  public String toString() {
    return "Copy Statement";
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
