package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTSingleChar extends SimpleNode {
  private char character;

  public ASTSingleChar(int id) {
    super(id);
  }

  public void setChar(char character) {
    this.character = character;
  }

  public char getChar() {
    return this.character;
  }

  public String toString() {
    return "Char: " + this.character;
  }

}
