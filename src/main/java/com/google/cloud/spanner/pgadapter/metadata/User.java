package com.google.cloud.spanner.pgadapter.metadata;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.MD5Digest;

/**
 * User class for when proper auth gets added.
 */
public class User {

  private static final Charset UTF8 = StandardCharsets.UTF_8;

  private final String username;
  private String md5Password;
  private int salt;

  public User(String username, String password) {
    Random rnd = new Random();
    this.salt = rnd.nextInt();
    byte[] saltBytes = new byte[4];
    ByteConverter.int4(saltBytes, 0, salt);
    this.username = username;
    this.md5Password = new String(
        MD5Digest.encode(username.getBytes(UTF8), password.getBytes(UTF8), saltBytes), UTF8);
  }

  public String getUsername() {
    return this.username;
  }

  public int getSalt() {
    return this.salt;
  }

  public boolean verify(String md5Password) {
    return this.md5Password.equals(md5Password);
  }
}

