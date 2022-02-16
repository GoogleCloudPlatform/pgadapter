package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTBoolean extends SimpleNode {
  private boolean bool;

  public ASTBoolean(int id) {
    super(id);
  }

  public ASTBoolean(Copy p, int id) {
    super(p, id);
  }

  public void setBool(boolean value) {
    this.bool = value;
  }

  public boolean getBool() {
    return this.bool;
  }

  public String toString() {
    return "Value: " + Boolean.toString(this.bool);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
