// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample.model;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Table(name = "lob_test")
@Entity
public class LobTest extends AbstractUuidEntity {

  @Basic(optional = true)
  @Lob
  @Column(length = 10000)
  private byte[] lobBytea;

  @Basic(optional = true)
  @Lob
  @Column(length = 10000)
  private byte[] lobOid;

  public byte[] getLobBytea() {
    return lobBytea;
  }

  public void setLobBytea(byte[] lobBytea) {
    this.lobBytea = lobBytea;
  }

  public byte[] getLobOid() {
    return lobOid;
  }

  public void setLobOid(byte[] lobOid) {
    this.lobOid = lobOid;
  }
}
