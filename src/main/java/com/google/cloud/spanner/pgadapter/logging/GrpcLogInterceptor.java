// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.logging;

import com.google.api.core.InternalApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.MethodDescriptor;
import java.util.logging.Level;
import java.util.logging.Logger;

/** gRPC interceptor for logging all messages that are sent by PGAdapter. */
@InternalApi
public class GrpcLogInterceptor implements ClientInterceptor {
  private static final Logger logger = Logger.getLogger(GrpcLogInterceptor.class.getName());

  private static final Printer PRINTER = JsonFormat.printer();

  private final Level level;

  public GrpcLogInterceptor(Level level) {
    this.level = level;
  }

  public static boolean isGrpcFinestLogEnabled() {
    return logger.isLoggable(Level.FINEST);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void sendMessage(ReqT message) {
        logger.log(level, () -> GrpcLogInterceptor.printMessage((MessageOrBuilder) message));
        super.sendMessage(message);
      }
    };
  }

  private static String printMessage(MessageOrBuilder message) {
    try {
      return message.getClass().getSimpleName() + " " + PRINTER.print(message);
    } catch (InvalidProtocolBufferException ignore) {
      return "(unknown message)";
    }
  }
}
