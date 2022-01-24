package com.google.cloud.spanner.pgadapter.parsers.copy;

public class CopyTreeParser implements CopyVisitor {
  public static class CopyOptions {
    public enum Format {
      TEXT,
      BINARY,
      CSV
    }

    public void setTableName(String name) {
      this.tableName = name;
    }

    public String getTableName() {
      return this.tableName;
    }

    public void setFormat(Format format) {
      this.format = format;
    }

    public Format getFormat() {
      return this.format;
    }

    public void setFrom(Boolean from) {
      this.from = from;
    }

    public boolean getFrom() {
      return this.from;
    }

    public void setDelimiter(char delimiter) {
      this.delimiter = delimiter;
    }

    public char getDelimiter() {
      return this.delimiter;
    }

    private String tableName;
    private Format format = Format.TEXT;
    private boolean from; // True == FROM, False == TO
    private char delimiter;
  }

  private CopyOptions options;

  public CopyTreeParser(CopyOptions options) {
    this.options = options;
  }

  public Object visit(SimpleNode node, Object data) {
    System.err.println(node + ": acceptor not unimplemented in subclass?");
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTStart node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyStatement node, Object data) {
    ASTID idNode = (ASTID) node.jjtGetChild(0).jjtGetChild(0);
    options.setTableName(idNode.getName());
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTID node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTFilename node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTColumnElement node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTColumnList node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTQualifiedName node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyFrom node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyOptions node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyOptionList node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyOptionElement node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }
}
