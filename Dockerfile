FROM scratch

ARG TARGETPLATFORM

COPY ./dist/${TARGETPLATFORM} /cache_cat

RUN chmod a+x /cache_cat

ENTRYPOINT ["/cache_cat"]
