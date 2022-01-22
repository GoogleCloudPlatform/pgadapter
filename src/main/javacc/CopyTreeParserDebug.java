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

  private int indent = 0;
  private CopyOptions options;

  public CopyTreeParser(CopyOptions options) {
    this.options = options;
  }

  private String indentString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < indent; ++i) {
      sb.append(' ');
    }
    return sb.toString();
  }

  public Object visit(SimpleNode node, Object data) {
    System.out.println(indentString() + node +
                   ": acceptor not unimplemented in subclass?");
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTStart node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTCopyStatement node, Object data) {
    ASTID idNode = (ASTID) node.jjtGetChild(0).jjtGetChild(0);
    options.setTableName(idNode.getName());
    System.out.println(indentString() + node + " : " + options.tableName);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTID node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTFilename node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTColumnElement node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTColumnList node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTQualifiedName node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTCopyFrom node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTCopyOptions node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTCopyOptionList node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }

  public Object visit(ASTCopyOptionElement node, Object data) {
    System.out.println(indentString() + node);
    ++indent;
    data = node.childrenAccept(this, data);
    --indent;
    return data;
  }
}
