FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
  build-essential cmake curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt /app/CMakeLists.txt
COPY include /app/include
COPY src /app/src
COPY third_party /app/third_party

RUN cmake -S . -B build && cmake --build build -j

EXPOSE 9000
CMD ["./build/engine"]
