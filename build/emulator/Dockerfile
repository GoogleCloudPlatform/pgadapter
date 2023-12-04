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

# Add the emulator binary.
COPY --from=emulator --chown=nonroot /emulator_main /emulator/emulator_main
COPY --from=emulator --chown=nonroot /licenses.txt.gz /emulator/licenses.txt.gz
RUN chmod u+x /emulator/emulator_main

# Expose both 5432 (PGAdapter PostgreSQL port) and 9010 (Emulator gRPC port)
EXPOSE 5432
EXPOSE 9010

ENTRYPOINT ["/bin/bash", "/home/pgadapter/startup.sh"]
