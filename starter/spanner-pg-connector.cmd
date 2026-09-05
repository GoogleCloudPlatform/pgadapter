@echo off
SET SCRIPT_DIR=%~dp0
SET JAR_PATH=%SCRIPT_DIR%pgadapter.jar
SET LIB_PATH=%SCRIPT_DIR%lib\*

SET JAVA_CMD="%SCRIPT_DIR%custom-jre\bin\java.exe"

REM Run the PGAdapter starter wrapper.
%JAVA_CMD% --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dio.netty.noUnsafe=true -Dio.grpc.netty.shaded.io.netty.noUnsafe=true -cp "%JAR_PATH%;%LIB_PATH%" com.google.cloud.spanner.pgadapter.SpannerPGConnector %*
