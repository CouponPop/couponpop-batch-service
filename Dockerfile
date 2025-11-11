# Dockerfile
# ---------- [1단계] Build Stage ----------
FROM gradle:8.7.0-jdk17-alpine AS builder
WORKDIR /app

# GitHub Packages 인증값을 빌드 인자/환경변수로 주입 (builder 단계 한정)
ARG GITHUB_ACTOR
ARG GITHUB_TOKEN
ENV GITHUB_ACTOR=${GITHUB_ACTOR}
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

# Gradle Wrapper 및 의존성 복사
COPY gradlew .
COPY gradle gradle
COPY build.gradle settings.gradle ./

# Gradle이 인증 정보를 읽을 수 있도록 secret을 마운트하고 환경 변수로 export 합니다.
RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN=$(cat /run/secrets/github_token) && \
    chmod +x ./gradlew && ./gradlew dependencies --no-daemon || return 0

# 소스 복사 및 빌드
COPY src src
# 빌드 시에도 동일하게 secret을 마운트합니다.
RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN=$(cat /run/secrets/github_token) && \
    ./gradlew clean bootJar --no-daemon


# ---------- [2단계] Runtime Stage ----------
FROM amazoncorretto:17-alpine-jdk

WORKDIR /app

# JAR 파일만 복사
COPY --from=builder /app/build/libs/*.jar app.jar

# 컨테이너 실행 포트
EXPOSE 8080

# Health check (ECS용)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl -f http://localhost:8080/actuator/health || exit 1

# 실행 명령
ENTRYPOINT ["java", "-jar", "/app/app.jar"]