package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTFormatType extends SimpleNode {
  private CopyTreeParser.CopyOptions.Format format;

  public ASTFormatType(int id) {
    super(id);
  }

  public ASTFormatType(Copy p, int id) {
    super(p, id);
  }

  public void setFormat(String format) {
    this.format = CopyTreeParser.CopyOptions.Format.valueOf(format);
  }

  public CopyTreeParser.CopyOptions.Format getFormat() {
    return this.format;
  }

  public String toString() {
    return "Format Type: " + format.toString();
  }

  /** Accept the visitor. * */
  public Object jjtAccept(CopyVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
