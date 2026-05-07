FROM scratch

ARG TARGETPLATFORM

COPY target/dist/${TARGETPLATFORM}/cache_cat /cache_cat

ENTRYPOINT ["/cache_cat"]