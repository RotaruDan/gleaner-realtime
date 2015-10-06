#
# Builds a jar-with-dependencies for the realtime analysis support
# Usage: 
#   mkdir output && chmod 0777 output \
#   && docker run -v $(pwd)/output:/app/output eucm/rage-analytics-realtime
#
FROM maven

ENV USER_NAME="user" \
    WORK_DIR="/app" \
    OUTPUT_VOL="output" \
    OUTPUT_JAR="target/realtime-jar-with-dependencies.jar"

# setup sources, user, group and workdir
COPY ./ ${WORK_DIR}/
RUN groupadd -r ${USER_NAME} \
    && useradd -r -d ${WORK_DIR} -g ${USER_NAME} ${USER_NAME} \
    && chown -R ${USER_NAME}:${USER_NAME} ${WORK_DIR}
ENV HOME=${WORK_DIR}
USER ${USER_NAME}
WORKDIR ${WORK_DIR}

# build, remove downloaded/unneeded jars, and expose results
RUN mvn install \
    && rm -rf .m2 \
    && mkdir ${OUTPUT_VOL}

VOLUME ${OUTPUT_VOL}

CMD cp ${OUTPUT_JAR} ${OUTPUT_VOL}
