FROM scratch

ARG TARGETPLATFORM

COPY target/dist/${TARGETPLATFORM}/core_raft /core_raft

ENTRYPOINT ["/core_raft"]