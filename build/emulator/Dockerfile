################################################################################
#                                   EMULATOR                                   #
################################################################################
FROM gcr.io/cloud-spanner-emulator/emulator AS emulator

################################################################################
#                                     PGADAPTER                                #
################################################################################
FROM gcr.io/cloud-spanner-pg-adapter/pgadapter AS pgadapter

# Replace the startup script with one that also starts the emulator.
ADD build/emulator/startup.sh /home/pgadapter/startup.sh
RUN chmod +x /home/pgadapter/startup.sh

# Add the emulator binaries.
COPY --from=emulator /gateway_main /emulator/gateway_main
COPY --from=emulator /emulator_main /emulator/emulator_main
COPY --from=emulator /licenses.txt.gz /emulator/licenses.txt.gz
RUN chmod u+x /emulator/gateway_main
RUN chmod u+x /emulator/emulator_main

# Expose 5432 (PGAdapter PostgreSQL port), 9010 (Emulator gRPC port), and 9020 (Emulator REST port)
EXPOSE 5432 9010 9020

ENTRYPOINT ["/bin/bash", "/home/pgadapter/startup.sh"]
