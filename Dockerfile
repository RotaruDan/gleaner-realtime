#
# Copyright © 2016 e-UCM (http://www.e-ucm.es/)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Builds a jar-with-dependencies for the realtime analysis support
# Usage: 
#   mkdir output && chmod 0777 output \
#   && docker run -v $(pwd)/output:/app/output eucm/rage-analytics-realtime
#
FROM maven:3-jdk-8-slim AS build

ARG BUILD_DIR="/scratch"

# setup sources, user, group and workdir
COPY ./ "${BUILD_DIR}"

# build, remove downloaded/unneeded jars, and expose results
RUN cd $BUILD_DIR \
    && mvn package -DskipTests=true -Dmaven.javadoc.skip=true -B -V \
    && mkdir -p "${BUILD_DIR}/volume/output" \
    && find "${BUILD_DIR}" -name '*.zip' -exec bash -c 'path={}; filename=$(basename "$path"); name="${filename%.*}"; unzip "$path" -d "${BUILD_DIR}/volume/output/$name"' \; \
    && find "${BUILD_DIR}/volume" -type d -exec chmod 777 {} \; \
    && find "${BUILD_DIR}/volume" -type f -exec chmod 666 {} \;


# Build a no-op static executable so the container can be run
# Based on: https://bitbucket.org/rw_grim/docker-noop/
FROM gcc:8 as build2
RUN apt-get update && apt-get install -y --no-install-recommends nasm
RUN mkdir /scratch
COPY noop-binary /scratch
WORKDIR /scratch
RUN make

FROM scratch
ARG OUTPUT_VOL="/analysis"
VOLUME ${OUTPUT_VOL}
COPY --from=build2 /scratch/noop /
COPY --from=build /scratch/volume ${OUTPUT_VOL}
ENTRYPOINT ["/noop"]
