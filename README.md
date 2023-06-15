# Pulsar Kotlin Example

An example of how to use Apache Pulsar in Kotlin.

## How to

### Build the project

```bash
./gradlew build

```

### Upgrade Gradle (example version)

```bash
./gradlew wrapper --gradle-version 8.1.1 --distribution-type all

```

### Update all dependencies if latest versions exist, and remove unused ones (it will update `gradle/libs.versions.toml`)

```bash
./gradlew versionCatalogUpdate

```