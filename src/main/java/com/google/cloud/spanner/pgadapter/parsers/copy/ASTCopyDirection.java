package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTCopyDirection extends SimpleNode {
  private CopyTreeParser.CopyOptions.FromTo direction;

  public ASTCopyDirection(int id) {
    super(id);
  }

  public ASTCopyDirection(Copy p, int id) {
    super(p, id);
  }

  public void setDirection(String direction) {
    this.direction = CopyTreeParser.CopyOptions.FromTo.valueOf(direction);
  }

  public CopyTreeParser.CopyOptions.FromTo getDirection() {
    return this.direction;
  }

  public String toString() {
    return "Copy Direction: " + direction.toString();
  }

  /** Accept the visitor. **/
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
