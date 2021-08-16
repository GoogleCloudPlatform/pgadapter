// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.metadata;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.MD5Digest;

/** User class for when proper auth gets added. */
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
    this.md5Password =
        new String(
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
