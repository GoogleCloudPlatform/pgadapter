# PGAdapter Developer & AI Assistant Instructions

This document provides developer guidelines, repository structure, branch invariants, and testing patterns for **PGAdapter**. If you are an AI assistant (e.g., Gemini, Jetski) or a human contributor, you **must** read and follow these rules to maintain code quality, ensure compatibility across language environments, and run tests correctly.

---

## 🚀 1. Critical Branch Invariant
- **Main Branch Name**: The default/main branch for this repository is **`postgresql-dialect`**.
- **Important**: NEVER compare feature branches or target pull requests against `main` or `master`. Always use `postgresql-dialect` as the base branch.

---

## 📦 2. Project Architecture & Repository Layout
PGAdapter is a stateful proxy written in **Java** that implements the PostgreSQL wire protocol, allowing clients to run PostgreSQL-compatible tools and drivers directly against Spanner PostgreSQL-dialect databases.

### Key Packages (`src/main/java`)
- **`com.google.cloud.spanner.pgadapter`**: Core server process, configuration, and entry points.
  - `Server.java`: The primary entry point (`main` method).
  - `ProxyServer.java`: Represents the lifecycle of the running PGAdapter proxy instance.
  - `ConnectionHandler.java`: Manages incoming TCP/socket client connections and stateful sessions.
- **`wireprotocol`**: Parsers and message definitions for handling incoming PostgreSQL frontend messages.
- **`wireoutput`**: Classes representing outgoing PostgreSQL backend messages sent back to clients.
- **`statements`**: Representation of queries, DML, DDL statements, parameter binding, statement caching, and translation.
- **`parsers`**: Handles parsing of commands, client-side virtual SQL, and translation logic.

### Multi-Language Integration & Wrappers
PGAdapter supports multiple programming languages through dynamic test environments and wrappers:
- **`wrappers/golang`**: Programmatic wrapper utility to start and stop PGAdapter from Go processes.
- **`samples/`**: Sample applications illustrating connections via JDBC, R2DBC, Hibernate, Spring Data JPA, pgx, GORM, Knex, Sequelize, Prisma, ActiveRecord, and psycopg.

---

## 🧪 3. Testing Architecture
PGAdapter utilizes a multi-layered testing strategy combining in-process mock servers, multi-language client library tests, and emulator integration tests.

### A. Mock Server Tests (Dialect & Wire Protocol Verification)
Classes inheriting from `AbstractMockServerTest` run in-memory gRPC servers:
1. **Mock Spanner Server** (`MockSpannerServiceImpl`): Implements the Spanner gRPC API. It is dialect-agnostic and returns pre-registered `StatementResult`s.
2. **In-process PGAdapter Proxy** (`ProxyServer`): Started on a random port. Tests connect to `pgServer.getLocalPort()` using various clients.
   - **Important**: Do not assume the proxy only serves JDBC/Java connections. Tests verify behavior across multiple PostgreSQL drivers in different language environments, such as:
     - Go (`pgx`, `GORM`)
     - Python (`psycopg2`, `psycopg3`, `SQLAlchemy`)
     - Node.js (`pg`, `Sequelize`, `Knex`, `Prisma`)
     - Ruby (`pg`, `ActiveRecord`)
     - .NET/C# (`Npgsql`)
- **Why**: Allows fast, robust testing of query translations and protocol error states without needing a real database.

### B. Multi-Language Client Library Tests (`src/test/`)
Integration with non-Java client drivers is verified by executing tests on-the-fly inside subprocesses:
- **Go (`src/test/golang`)**: Go tests (e.g. `gorm.go`) are compiled into `.so` shared libraries using `go build -buildmode=c-shared` and loaded dynamically into JUnit tests via **JNA** (`com.sun.jna.Native`).
- **Python (`src/test/python`)**: Automatically initializes a python virtual environment (`venv`), upgrades `pip`, installs `requirements.txt`, and runs tests.
- **Node.js (`src/test/nodejs`)**: Executed via custom npm scripts and uses **Testcontainers** under the hood.
- **Ruby (`src/test/ruby`)**: Run via Bundler (`bundle install`, `bundle exec`).
- **.NET/C# (`src/test/csharp`)**: Run via `dotnet run`.

### C. Integration Tests (`IT*Test.java`)
These tests run end-to-end against either a local Spanner Emulator or a real Spanner database instance, comparing behaviors side-by-side with a real local PostgreSQL database instance.

---

## 🛠️ 4. How to Build & Run Tests

### Command Guide

| Test Scope | Command | Details |
| :--- | :--- | :--- |
| **Java Unit Tests** | `mvn test` | Excludes client library tests and E2E integration tests. |
| **All Mock Client Tests** | `mvn test -Ptest-all` | Includes Go, Python, Node, Ruby, and .NET mock client tests. |
| **Emulator Integration Tests** | `mvn verify -DskipUnits=true -DskipITs=false` | Requires running Spanner Emulator and local PostgreSQL instance. |

### Environment Variables for Integration Tests
To run integration tests locally, configure these variables:
- `SPANNER_EMULATOR_HOST=localhost:9010`
- `POSTGRES_HOST=localhost` (or path to UNIX socket `/pg` for socket connections)
- `POSTGRES_PORT=5432`
- `POSTGRES_USER=postgres`
- `POSTGRES_DATABASE=pgadapter`
- `PG_ADAPTER_USE_EXISTING_DB=true`: Optionally reuse an existing database to significantly speed up successive local test runs.

---

## 📌 5. General Coding Guidelines & Formatting
- **Java Code Formatting**: This repository uses a strict style guide formatted by `fmt-maven-plugin`. You can run the code formatter using:
  ```shell
  mvn fmt:format
  ```
  Ensure all files are formatted before committing or submitting changes.

---

## 🎯 6. Client Compatibility & Alignment Philosophy
PGAdapter is designed to be a transparent, drop-in proxy for PostgreSQL. When integrating or supporting any client, driver, ORM, or framework (e.g., Prisma, SQLAlchemy, pgx, ActiveRecord, gorm), you must follow these rules:

1. **Maximize Client Defaults**: Always design sample apps and test expectations around the **standard and default way** the client connects to a regular PostgreSQL instance. Do not introduce custom client configurations, driver adapters, or query overrides unless there is absolutely no other way.
2. **Fix on Proxy Side**: If a client encounters wire protocol failures, unsupported SQL commands, or missing metadata/system catalog queries, **always resolve the issue by modifying PGAdapter** (e.g., implementing missing messages, extending the translation engine, or providing simulated catalog tables/views). Do not work around proxy deficiencies by changing the client application.
3. **Ensure Drop-in Swap**: The ultimate goal is for users to be able to point their PostgreSQL applications to Spanner PGAdapter by simply modifying the connection string, with zero changes to their code.

