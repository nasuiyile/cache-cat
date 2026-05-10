FROM scratch

ARG TARGETPLATFORM

COPY --chmod=0777 ./dist/${TARGETPLATFORM} /cache_cat


ENTRYPOINT ["/cache_cat"]
