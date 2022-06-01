package com.google.cloud.spanner.pgadapter.python;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PythonTest {
  static String execute(String test)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    String[] runCommand = new String[] {"python", "pgAdapterBasicTests.py", test};
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(output.toString(), 0, result);

    return output.toString();
  }





}
