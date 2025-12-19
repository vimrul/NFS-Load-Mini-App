FROM ubuntu:22.04
RUN apt-get update && apt-get install -y fio ca-certificates && rm -rf /var/lib/apt/lists/*
CMD ["fio", "--version"]
