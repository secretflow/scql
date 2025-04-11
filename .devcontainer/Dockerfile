FROM secretflow/ubuntu-base-ci:latest

ARG TARGETPLATFORM
ARG GO_VERSION=1.24.0

# install go
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ] ; \
    then \
        GO_ARCH=arm64 && \
        GO_SHA256SUM=c3fa6d16ffa261091a5617145553c71d21435ce547e44cc6dfb7470865527cc7 ; \
    else \
        GO_ARCH=amd64 && \
        GO_SHA256SUM=dea9ca38a0b852a74e81c26134671af7c0fbe65d81b0dc1c5bfe22cf7d4c8858 ; \
    fi \
    && url="https://golang.google.cn/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"; \
    wget --no-check-certificate -O go.tgz "$url"; \
    echo "${GO_SHA256SUM} *go.tgz" | sha256sum -c -; \
    tar -C /usr/local -xzf go.tgz; \
    rm go.tgz;

ENV GOPATH="/usr/local"
ENV PATH="/usr/local/go/bin:${GOPATH}/bin:${PATH}"

RUN apt update \
    && apt upgrade -y \
    && apt install -y protobuf-compiler \
    && apt clean


# Create a non-root user 'vscode' with sudo privileges
# The user ID 1000 is common for the primary user in Linux distributions.
# The GID 1000 corresponds to the user's primary group.
RUN groupadd --gid 1000 vscode && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home vscode && \
    echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set the default user for subsequent commands
USER vscode

# Set the working directory inside the container
WORKDIR /workspaces/scql

# Set default shell to bash for the vscode user
ENV SHELL /bin/bash

# You can add more SCQL-specific dependencies here if needed,
# although 'make install-dev-deps' in postCreateCommand should handle most project dependencies.

# Keep the container running (optional, useful for debugging setup)
# CMD ["sleep", "infinity"]
