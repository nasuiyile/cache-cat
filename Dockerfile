FROM scratch

ARG TARGETPLATFORM

COPY --chmod=0777 ./dist/${TARGETPLATFORM} /cache_cat
COPY ./cache_cat/conf/docker.toml /etc/cache-cat.toml

# Port of Redis
EXPOSE 6379 

# Port of raft
EXPOSE 5001

ENTRYPOINT ["/cache_cat","--conf","/etc/cache-cat.toml"]
