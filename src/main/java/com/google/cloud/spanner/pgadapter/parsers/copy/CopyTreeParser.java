package com.google.cloud.spanner.pgadapter.parsers.copy;

import java.util.ArrayList;
import java.util.List;

public class CopyTreeParser implements CopyVisitor {
  public static class CopyOptions {
    public enum Format {
      TEXT,
      BINARY,
      CSV
    }

    public enum FromTo {
      FROM,
      TO
    }

    public void addColumnName(String name) {
      columnNames.add(name);
    }

    public List<String> getColumnNames() {
      return this.columnNames;
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

    public void setFromTo(FromTo direction) {
      this.direction = direction;
    }

    public FromTo getFromTo() {
      return this.direction;
    }

    public void setHeader(boolean header) {
      this.header = header;
    }

    public boolean hasHeader() {
      return this.header;
    }

    public void setDelimiter(char delimiter) {
      this.delimiter = delimiter;
    }

    public char getDelimiter() {
      return this.delimiter;
    }

    public void setEscape(char escape) {
      this.escape = escape;
    }

    public char getEscape() {
      return this.escape;
    }

    public void setQuote(char quote) {
      this.quote = quote;
    }

    public char getQuote() {
      return this.quote;
    }

    public void setNullString(String nullString) {
      this.nullString = nullString;
    }

    public String getNullString() {
      return this.nullString;
    }

    private List<String> columnNames = new ArrayList<String>();
    private String tableName = "";
    private Format format = Format.TEXT;
    private FromTo direction;
    private boolean header;
    private char delimiter;
    private char escape;
    private char quote;
    private String nullString;
  }

  private CopyOptions options;

  public CopyTreeParser(CopyOptions options) {
    this.options = options;
  }

  public Object visit(SimpleNode node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTStart node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyStatement node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTID node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTSingleChar node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTFilename node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTFormatType node, Object data) {
    options.setFormat(node.getFormat());
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTColumnElement node, Object data) {
    ASTID columnNode = (ASTID) node.jjtGetChild(0);
    options.addColumnName(columnNode.getName());
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTColumnList node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTQualifiedName node, Object data) {
    // Zero or more namespaces can prefix the identifier.
    // (<namespace>.)*table
    String prefix = "";
    ASTNamespace currentNode = (ASTNamespace) node.jjtGetChild(0);
    while (currentNode.jjtGetNumChildren() > 1) {
      prefix += (((ASTID) currentNode.jjtGetChild(0)).getName() + ".");
      currentNode = (ASTNamespace) currentNode.jjtGetChild(1);
    }
    options.setTableName(prefix + ((ASTID) currentNode.jjtGetChild(0)).getName());
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTNamespace node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTCopyDirection node, Object data) {
    options.setFromTo(node.getDirection());
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
    switch (node.getName()) {
      case "DELIMITER":
        {
          ASTSingleChar charNode = (ASTSingleChar) node.jjtGetChild(0);
          options.setDelimiter(charNode.getChar());
        }
        break;
      case "NULL":
        {
          ASTID idNode = (ASTID) node.jjtGetChild(0);
          options.setNullString(idNode.getName());
        }
        break;
      case "HEADER":
        {
          ASTBoolean boolNode = (ASTBoolean) node.jjtGetChild(0);
          options.setHeader(boolNode.getBool());
        }
        break;
      case "QUOTE":
        {
          ASTSingleChar charNode = (ASTSingleChar) node.jjtGetChild(0);
          options.setQuote(charNode.getChar());
        }
        break;
      case "ESCAPE":
        {
          ASTSingleChar charNode = (ASTSingleChar) node.jjtGetChild(0);
          options.setEscape(charNode.getChar());
        }
        break;
      default:
        break;
    }
    data = node.childrenAccept(this, data);
    return data;
  }

  public Object visit(ASTBoolean node, Object data) {
    data = node.childrenAccept(this, data);
    return data;
  }
}
