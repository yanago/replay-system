# =============================================================================
# Replay System — Multi-stage Dockerfile
# =============================================================================
# Stage 1 builds the fat JAR.
# Stage 2 produces a minimal JRE image with a non-root user.
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Download dependencies before copying source so this layer is cached
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src/ src/
RUN mvn package -DskipTests -q \
 && cp $(ls target/replay-system-*.jar | grep -v original | head -1) target/app.jar

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

LABEL org.opencontainers.image.title="Replay System" \
      org.opencontainers.image.description="Security-event replay API server" \
      org.opencontainers.image.source="https://github.com/yanago/replay-system"

WORKDIR /app

# Non-root user
RUN addgroup -S replay && adduser -S replay -G replay

# Writable directories the app needs
RUN mkdir -p /data/iceberg-warehouse /app/logs \
 && chown -R replay:replay /data /app/logs

COPY --from=builder --chown=replay:replay /build/target/app.jar replay-system.jar

USER replay

EXPOSE 8080

# Sensible defaults — every value is overridable via env/k8s ConfigMap/Secret
ENV HTTP_PORT=8080 \
    JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75"

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD wget -qO- http://localhost:${HTTP_PORT}/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Xss512k -jar /app/replay-system.jar"]
