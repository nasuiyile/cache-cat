FROM ubuntu AS build
ARG TARGETPLATFORM
ENV TARGETPLATFORM=${TARGETPLATFORM}

# SHELL [ "bash","-C" ]

WORKDIR /install-bin

RUN --mount=type=bind,source=.,target=/host \
  TVAL_NAME=$(echo $TARGETPLATFORM | sed 's/\//-/g'); \
  find /host; \
  echo "Install: $TVAL_NAME"; \
  cp -v /host/cache_cat-$TVAL_NAME/* /install-bin/;


FROM scratch

COPY --chmod=0777 --from=build /install-bin/* /cache-cat/
RUN chmod 777 /cache-cat/*
# Port of Redis
EXPOSE 6379 

# Port of raft
EXPOSE 5001

# Health Check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/cache-cat/cache_cat_ping" ,"127.0.0.1", "6379"]

# RUN ls /cache-cat
# RUN /cache-cat/cache_cat --help

ENTRYPOINT ["/cache-cat/cache_cat","--advertise-host","0.0.0.0"]
