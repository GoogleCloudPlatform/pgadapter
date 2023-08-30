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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.util.List;

@Table(name = "singers")
@Entity
public class Singer extends AbstractUuidEntity {

  @Column(length = 100)
  private String firstName;

  @Column(length = 200)
  private String lastName;

  @Column(
      columnDefinition =
          "varchar(300) generated always as (\n"
              + "CASE WHEN first_name IS NULL THEN last_name\n"
              + "     WHEN last_name IS NULL THEN first_name\n"
              + "     ELSE first_name || ' ' || last_name\n"
              + "END) stored",
      insertable = false,
      updatable = false)
  private String fullName;

  private boolean active;

  @OneToMany
  @JoinColumn(name = "singer_id")
  private List<Album> albums;

  @OneToMany
  @JoinColumn(name = "singer_id")
  private List<Concert> concerts;

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getFullName() {
    return fullName;
  }

  public void setFullName(String fullName) {
    this.fullName = fullName;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public List<Album> getAlbums() {
    return albums;
  }

  public void setAlbums(List<Album> albums) {
    this.albums = albums;
  }

  public List<Concert> getConcerts() {
    return concerts;
  }

  public void setConcerts(List<Concert> concerts) {
    this.concerts = concerts;
  }
}
