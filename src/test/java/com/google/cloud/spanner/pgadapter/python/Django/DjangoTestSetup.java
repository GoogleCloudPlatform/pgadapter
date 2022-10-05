package com.google.cloud.spanner.pgadapter.python.Django;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DjangoTestSetup extends DjangoMockServerTest {

  static boolean isPythonAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] pythonCommand = new String[] {"python3", "--version"};
    builder.command(pythonCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static String DJANGO_PATH = "./src/test/python/django";

  public String executeBasicTests(int port, String host, List<String> options)
      throws IOException, InterruptedException {
    List<String> runCommand =
        new ArrayList<>(Arrays.asList("python3", "basic_test.py", host, Integer.toString(port)));
    runCommand.addAll(options);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File(DJANGO_PATH));
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
