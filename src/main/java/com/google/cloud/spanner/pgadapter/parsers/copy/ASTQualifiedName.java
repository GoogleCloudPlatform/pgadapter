/* Generated By:JJTree: Do not edit this line. ASTQualifiedName.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.google.cloud.spanner.pgadapter.parsers.copy;

public class ASTQualifiedName extends SimpleNode {
  public ASTQualifiedName(int id) {
    super(id);
  }

  public ASTQualifiedName(Copy p, int id) {
    super(p, id);
  }

  /** Accept the visitor. * */
  public Object jjtAccept(CopyVisitor visitor, Object data) {

    return visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=b7a5512829c8af499f47d7757ccd39ae (do not edit this line) */
