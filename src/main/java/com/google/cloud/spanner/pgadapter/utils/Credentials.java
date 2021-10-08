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

package com.google.cloud.spanner.pgadapter.utils;

import java.io.File;
import java.util.Locale;

/**
 * Simple Google Cloud Credential Utilities. Similar code exists in GoogleCredentials, but does not
 * provide access to the credentials file path, which is what we need here.
 */
public class Credentials {

  private static final String CREDENTIAL_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String CLOUD_SDK_ENV_VAR = "CLOUDSDK_CONFIG";
  private static final String WINDOWS_ENV_VAR = "APPDATA";
  private static final String CLOUDSDK_CONFIG_DIRECTORY = "gcloud";
  private static final String UNIX_HOME_PROPERTY = "user.home";
  private static final String UNIX_CONFIG_PROPERTY = ".config";
  private static final String OS_NAME_PROPERTY = "os.name";
  private static final String WINDOWS_OS = "windows";
  private static final String CREDENTIAL_FILE_NAME = "application_default_credentials.json";

  /**
   * Returns the application default credential file path (if any) by looking in order at: *
   * Environment variables * OS files Once the paths are constructed, existence is captured by
   * whether the file itself exists. If none are found, returns null to signify none exist.
   *
   * @return The absolute path to the application default credential file, or null if none.
   */
  public static String getApplicationDefaultCredentialsFilePath() {
    File[] credentialFiles = {
      getDefaultCredentialFilePath(), getCloudSDKCredentialFile(), getOSCredentialFile()
    };

    for (File currentCredentialsFile : credentialFiles) {
      if (credentialExists(currentCredentialsFile)) {
        return currentCredentialsFile.getAbsolutePath();
      }
    }
    return null;
  }

  /**
   * Simple getter for the default user-defined credential env path.
   *
   * @return The File object representing the user-defined path.
   */
  private static File getDefaultCredentialFilePath() {
    return getEnvironmentVariableFile(CREDENTIAL_ENV_VAR);
  }

  /**
   * Simple getter for the default sdk-defined credential env path.
   *
   * @return The File object representing the sdk-defined path.
   */
  private static File getCloudSDKCredentialFile() {
    return appendFileNameToDirectory(getEnvironmentVariableFile(CLOUD_SDK_ENV_VAR));
  }

  /**
   * Gets the File object for a speficied env key if any, else returns null.
   *
   * @param environmentVariableKey The env key corresponding to a credential path.
   * @return The designated File object or null if the env key doesn't exist.
   */
  private static File getEnvironmentVariableFile(String environmentVariableKey) {
    String environmentVariable = System.getenv(environmentVariableKey);
    if (environmentVariable != null) {
      return new File(environmentVariable);
    }
    return null;
  }

  /**
   * The directory containing application default credentials in Windows.
   *
   * @return The default directory as a File object.
   */
  private static File getWindowsCredentialDirectory() {
    File appDataPath = new File(System.getenv(WINDOWS_ENV_VAR));
    return new File(appDataPath, CLOUDSDK_CONFIG_DIRECTORY);
  }

  /**
   * The directory containing application default credentials in Unix.
   *
   * @return The default directory as a File object.
   */
  private static File getUnixCredentialDirectory() {
    File configPath = new File(System.getProperty(UNIX_HOME_PROPERTY, ""), UNIX_CONFIG_PROPERTY);
    return new File(configPath, CLOUDSDK_CONFIG_DIRECTORY);
  }

  /**
   * Construct the credentials directory for the relevant OS and append the filename to it.
   *
   * @return the File representing the credentials for the specific OS.
   */
  private static File getOSCredentialFile() {
    String os = System.getProperty(OS_NAME_PROPERTY, "").toLowerCase(Locale.US);
    File credentialsDirectory;
    if (os.contains(WINDOWS_OS)) {
      credentialsDirectory = getWindowsCredentialDirectory();
    } else {
      credentialsDirectory = getUnixCredentialDirectory();
    }
    return appendFileNameToDirectory(credentialsDirectory);
  }

  /**
   * Appends the default filename to the directory containing the credentials or returns null if the
   * directory is null.
   *
   * @param directory The directory to append the default filename to.
   * @return A File representing the credentials.
   */
  private static File appendFileNameToDirectory(File directory) {
    if (directory != null) {
      return new File(directory, CREDENTIAL_FILE_NAME);
    }
    return null;
  }

  /**
   * Whether the credentials File exists.
   *
   * @param credentialFile File object where the credentials should be housed.
   * @return True if the file exists, false otherwise.
   */
  private static boolean credentialExists(File credentialFile) {
    return (credentialFile != null) && (credentialFile.exists());
  }
}
