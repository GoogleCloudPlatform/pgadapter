package com.google.cloud.postgres.models;

import java.io.Serializable;

public class VenueDescription implements Serializable {

  private String name;
  private String val;

  public VenueDescription() {
  }

  public VenueDescription(String name, String val) {
    this.name = name;
    this.val = val;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}
