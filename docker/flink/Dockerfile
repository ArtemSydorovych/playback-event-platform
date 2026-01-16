# Production Flink Application Mode Image
# Build: docker build -t playback-flink-job -f docker/flink/Dockerfile .
# Run:   docker run playback-flink-job

FROM flink:1.18-java17

# Copy the job JAR
COPY flink-jobs/build/libs/playback-flink-jobs-all.jar /opt/flink/usrlib/playback-flink-jobs-all.jar

# Set the entrypoint class
ENV FLINK_PROPERTIES="jobmanager.rpc.address: localhost"

# Application mode: job runs as part of cluster startup
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["standalone-job", "--job-classname", "com.artemsydorovych.playback.flink.PlaybackPipelineJob"]
