FROM scratch

ARG TARGETPLATFORM

COPY --chmod=0777 ./dist/${TARGETPLATFORM} /cache_cat
COPY ./cache_cat/conf/node1.toml /etc/cache-cat.toml


ENTRYPOINT ["/cache_cat","--conf","/etc/cache-cat.toml"]
