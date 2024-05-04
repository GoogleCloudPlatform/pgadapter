# Changelog

## [0.33.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.32.0...v0.33.0) (2024-05-01)


### Features

* log info to stdout and warnings and errors to stderr ([#1626](https://github.com/GoogleCloudPlatform/pgadapter/issues/1626)) ([19fbe8e](https://github.com/GoogleCloudPlatform/pgadapter/commit/19fbe8e53dc4d22f9f62efefe082c89c76e684eb))


### Bug Fixes

* add a random service name for OpenTelemetry ([#1629](https://github.com/GoogleCloudPlatform/pgadapter/issues/1629)) ([3cfabdd](https://github.com/GoogleCloudPlatform/pgadapter/commit/3cfabdd6d70b90a87c871dcbfba5df90e4433c42))
* add default replacement for obj_description ([#1686](https://github.com/GoogleCloudPlatform/pgadapter/issues/1686)) ([dc0555b](https://github.com/GoogleCloudPlatform/pgadapter/commit/dc0555b624d13131db0b60753d49b2bdd9d6408d))
* fix auto-detection of npgsql on Windows ([#1674](https://github.com/GoogleCloudPlatform/pgadapter/issues/1674)) ([6a31bb9](https://github.com/GoogleCloudPlatform/pgadapter/commit/6a31bb96c5f89d5b9258f55f37d95daab3cab8f5))
* skip test on emulator ([444e133](https://github.com/GoogleCloudPlatform/pgadapter/commit/444e133a2bde5a6851ff6849981896e32e89d748))


### Dependencies

* update alpine docker tag ([#1655](https://github.com/GoogleCloudPlatform/pgadapter/issues/1655)) ([0d6b9ec](https://github.com/GoogleCloudPlatform/pgadapter/commit/0d6b9ec38edbebcffa3461cece8d69bfbe55ba8c))
* update codecov/codecov-action action to v4 ([#1687](https://github.com/GoogleCloudPlatform/pgadapter/issues/1687)) ([825348c](https://github.com/GoogleCloudPlatform/pgadapter/commit/825348c89776e197c812b1710d2653a781b2819a))
* update dependency @google-cloud/spanner to v7 ([#1688](https://github.com/GoogleCloudPlatform/pgadapter/issues/1688)) ([e72bb70](https://github.com/GoogleCloudPlatform/pgadapter/commit/e72bb7040cf9f6b438622b7925ecd9a82ceccf13))
* update dependency com.google.cloud.tools:jib-maven-plugin to v3.4.2 ([#1656](https://github.com/GoogleCloudPlatform/pgadapter/issues/1656)) ([cc82ef7](https://github.com/GoogleCloudPlatform/pgadapter/commit/cc82ef772bb645be6ec05401e8c4fcdc56fd5992))
* update dependency django to ~=4.2.11 ([#1657](https://github.com/GoogleCloudPlatform/pgadapter/issues/1657)) ([e6e0144](https://github.com/GoogleCloudPlatform/pgadapter/commit/e6e0144715d592cb80f46d2d5eeb731b69e3ce26))
* update dependency flask to v2.3.3 ([#1667](https://github.com/GoogleCloudPlatform/pgadapter/issues/1667)) ([3268268](https://github.com/GoogleCloudPlatform/pgadapter/commit/3268268db0b3c88778e2d3a109846758f7118d0a))
* update dependency flask to v3 ([#1690](https://github.com/GoogleCloudPlatform/pgadapter/issues/1690)) ([1ec7503](https://github.com/GoogleCloudPlatform/pgadapter/commit/1ec7503df6cb31745d162b5f47a9f8e93cc827ab))
* update dependency google.auth to ~=2.29.0 ([#1668](https://github.com/GoogleCloudPlatform/pgadapter/issues/1668)) ([f44b056](https://github.com/GoogleCloudPlatform/pgadapter/commit/f44b0567bf261d706d4640fcd3cdea6613d40b22))
* update dependency io.hypersistence:hypersistence-utils-hibernate-63 to v3.7.5 ([#1676](https://github.com/GoogleCloudPlatform/pgadapter/issues/1676)) ([bc9fd09](https://github.com/GoogleCloudPlatform/pgadapter/commit/bc9fd09a4a2a3faff6c1e3456d0b7973523b3807))
* update dependency org.postgresql:postgresql to v42.7.3 ([#1648](https://github.com/GoogleCloudPlatform/pgadapter/issues/1648)) ([72c5c26](https://github.com/GoogleCloudPlatform/pgadapter/commit/72c5c260a8c867418433ff3abcf062cdb3fee29a))
* update dependency sinatra to v4 ([#1695](https://github.com/GoogleCloudPlatform/pgadapter/issues/1695)) ([ed9f78c](https://github.com/GoogleCloudPlatform/pgadapter/commit/ed9f78c0c46f56553239e2c5af9886c4bbdfdf7c))
* update dependency sqlalchemy to v1.4.52 ([#1649](https://github.com/GoogleCloudPlatform/pgadapter/issues/1649)) ([333facb](https://github.com/GoogleCloudPlatform/pgadapter/commit/333facb25c1e28bffd35f4a65eb28d496e47a872))
* update dependency ts-node to v10.9.2 ([#1650](https://github.com/GoogleCloudPlatform/pgadapter/issues/1650)) ([27b4262](https://github.com/GoogleCloudPlatform/pgadapter/commit/27b4262da604c3cefdb9b168fbf98508c60afc3d))
* update dependency typescript to v5.4.5 ([#1670](https://github.com/GoogleCloudPlatform/pgadapter/issues/1670)) ([950209f](https://github.com/GoogleCloudPlatform/pgadapter/commit/950209f34e3a9d05bf0a2ff3ea8ee06de339ae82))
* update golang docker tag ([#1671](https://github.com/GoogleCloudPlatform/pgadapter/issues/1671)) ([def1e60](https://github.com/GoogleCloudPlatform/pgadapter/commit/def1e602f4fcd9d129e915c38e6df2a197b27954))
* update junixsocket.version to v2.9.1 ([#1672](https://github.com/GoogleCloudPlatform/pgadapter/issues/1672)) ([d469790](https://github.com/GoogleCloudPlatform/pgadapter/commit/d469790b8c6345a0a668acd6a5e114242b1fb906))
* update maven docker tag to v3.9.6 ([#1651](https://github.com/GoogleCloudPlatform/pgadapter/issues/1651)) ([c836a30](https://github.com/GoogleCloudPlatform/pgadapter/commit/c836a30709ddc7f211613ae3a1c0be0033b9688c))
* update module cloud.google.com/go/spanner to v1.60.0 ([#1673](https://github.com/GoogleCloudPlatform/pgadapter/issues/1673)) ([df6445b](https://github.com/GoogleCloudPlatform/pgadapter/commit/df6445b05e8f568d6bbdcba47093847e2130ec5f))
* update module cloud.google.com/go/spanner to v1.61.0 ([#1682](https://github.com/GoogleCloudPlatform/pgadapter/issues/1682)) ([2753893](https://github.com/GoogleCloudPlatform/pgadapter/commit/2753893d6b86b57313c3e559efc7528ca4e01fac))
* update module github.com/jackc/pgx/v4 to v4.18.3 ([#1652](https://github.com/GoogleCloudPlatform/pgadapter/issues/1652)) ([08d8d52](https://github.com/GoogleCloudPlatform/pgadapter/commit/08d8d529cced892eb29500fb9517e907d2869916))
* update module github.com/jackc/pgx/v5 to v5.5.5 ([#1653](https://github.com/GoogleCloudPlatform/pgadapter/issues/1653)) ([a916cff](https://github.com/GoogleCloudPlatform/pgadapter/commit/a916cff9c545fcbb53816b98398f16c504c89f99))
* update module github.com/testcontainers/testcontainers-go to v0.30.0 ([#1680](https://github.com/GoogleCloudPlatform/pgadapter/issues/1680)) ([b34aeac](https://github.com/GoogleCloudPlatform/pgadapter/commit/b34aeac7e06a1fbe6061ab8923deffe854dcd054))
* update module golang.org/x/oauth2 to v0.19.0 ([#1681](https://github.com/GoogleCloudPlatform/pgadapter/issues/1681)) ([076db91](https://github.com/GoogleCloudPlatform/pgadapter/commit/076db91cf055925b49250e440b9c88d723b29a59))
* update module google.golang.org/api to v0.176.1 ([#1683](https://github.com/GoogleCloudPlatform/pgadapter/issues/1683)) ([16501dd](https://github.com/GoogleCloudPlatform/pgadapter/commit/16501dd5bb1ce6430a4f2a43ccb34b3e5fce75ab))
* update module google.golang.org/api to v0.177.0 ([#1697](https://github.com/GoogleCloudPlatform/pgadapter/issues/1697)) ([9be8009](https://github.com/GoogleCloudPlatform/pgadapter/commit/9be80092bf740b9d219fab79d469587915488d81))
* update python docker tag to v3.12 ([#1685](https://github.com/GoogleCloudPlatform/pgadapter/issues/1685)) ([296bf3a](https://github.com/GoogleCloudPlatform/pgadapter/commit/296bf3a1994af4906778f3b0af51afc9a2194dc5))
* update spring-boot.version to v2.7.18 ([#1654](https://github.com/GoogleCloudPlatform/pgadapter/issues/1654)) ([b886b67](https://github.com/GoogleCloudPlatform/pgadapter/commit/b886b67afa1991b92a46a3d34e2aaf1c923a94b7))


### Documentation

* add npgsql sample with emulator ([#1664](https://github.com/GoogleCloudPlatform/pgadapter/issues/1664)) ([a550255](https://github.com/GoogleCloudPlatform/pgadapter/commit/a550255d80ac4f795648af9e413c88f536679e35))

## [0.32.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.31.1...v0.32.0) (2024-03-26)


### Features

* support client lib's OpenTelemetry metrics ([#1561](https://github.com/GoogleCloudPlatform/pgadapter/issues/1561)) ([027417d](https://github.com/GoogleCloudPlatform/pgadapter/commit/027417d6dbdc2a5da21b45ca475413adfd7a13f1))
* support float4 data type ([#1481](https://github.com/GoogleCloudPlatform/pgadapter/issues/1481)) ([c2554fc](https://github.com/GoogleCloudPlatform/pgadapter/commit/c2554fc380f34e8dfffaea988df43d1c2d9580bb))


### Documentation

* document how to get 32-bit integers in node-postgres ([#1512](https://github.com/GoogleCloudPlatform/pgadapter/issues/1512)) ([d49e678](https://github.com/GoogleCloudPlatform/pgadapter/commit/d49e6781fce027436e08d81219c5d00c407b90a2))
* sleep 2s to ensure PGAdapter has started ([#1533](https://github.com/GoogleCloudPlatform/pgadapter/issues/1533)) ([8b1475c](https://github.com/GoogleCloudPlatform/pgadapter/commit/8b1475c63321fbf3d2cc2f0a89ac207be7bbcdd1))
* support read/write tx in latency comparison ([#1532](https://github.com/GoogleCloudPlatform/pgadapter/issues/1532)) ([7c14284](https://github.com/GoogleCloudPlatform/pgadapter/commit/7c1428441406a386f6db11a3d0a54260bacae83a))

## [0.31.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.31.0...v0.31.1) (2024-03-07)


### Bug Fixes

* boolean parser was case sensitive ([#1490](https://github.com/GoogleCloudPlatform/pgadapter/issues/1490)) ([5e97378](https://github.com/GoogleCloudPlatform/pgadapter/commit/5e97378775e63e5786dd30bdf55aa7f29da91d76))

## [0.31.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.30.0...v0.31.0) (2024-02-21)


### Features

* support set_config and current_setting ([#1437](https://github.com/GoogleCloudPlatform/pgadapter/issues/1437)) ([59d3250](https://github.com/GoogleCloudPlatform/pgadapter/commit/59d3250467ec988d546082e3fce9e955019ca5b4))


### Bug Fixes

* respect the SPANNER_EMULATOR_HOST env var ([#1432](https://github.com/GoogleCloudPlatform/pgadapter/issues/1432)) ([2f059f7](https://github.com/GoogleCloudPlatform/pgadapter/commit/2f059f7dc034a546bd3244923b159cfd2a0259d7))


### Dependencies

* remove junit dependency from compile path ([#1423](https://github.com/GoogleCloudPlatform/pgadapter/issues/1423)) ([9c315d1](https://github.com/GoogleCloudPlatform/pgadapter/commit/9c315d1ed41b97ffef69492d21b7939803bdd512))


### Documentation

* Ruby ActiveRecord bit-reversed sequence sample ([#1434](https://github.com/GoogleCloudPlatform/pgadapter/issues/1434)) ([176dd15](https://github.com/GoogleCloudPlatform/pgadapter/commit/176dd150f3e5baf5fbd8a37ef7a63edfa49d1a28))
* run Ruby ActiveRecord sample on emulator ([#1433](https://github.com/GoogleCloudPlatform/pgadapter/issues/1433)) ([cd6c9dc](https://github.com/GoogleCloudPlatform/pgadapter/commit/cd6c9dc9ae3492b9c561ca892cbbf518645be623))

## [0.30.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.29.1...v0.30.0) (2024-02-16)


### Features

* add sample for bit-reversed sequence with Hibernate / JPA ([#1387](https://github.com/GoogleCloudPlatform/pgadapter/issues/1387)) ([216efa6](https://github.com/GoogleCloudPlatform/pgadapter/commit/216efa6f4dd20ed322d0c7e4798752089a2dfa4b))
* bit-reversed sequences in SQLAlchemy ([#1405](https://github.com/GoogleCloudPlatform/pgadapter/issues/1405)) ([6a7d393](https://github.com/GoogleCloudPlatform/pgadapter/commit/6a7d393d5896d0c119c6ae9a4cc8b62dc67ea239))
* document and test ARRAY mapping for gorm ([#1400](https://github.com/GoogleCloudPlatform/pgadapter/issues/1400)) ([968ac2d](https://github.com/GoogleCloudPlatform/pgadapter/commit/968ac2daca2b07a7056df688a7986e565535f16d))
* gorm nested transactions ([#1401](https://github.com/GoogleCloudPlatform/pgadapter/issues/1401)) ([2636a49](https://github.com/GoogleCloudPlatform/pgadapter/commit/2636a49415061bb3b0e392d143e78a20dcd5e400))
* run SQLAlchemy2 sample on emulator ([#1403](https://github.com/GoogleCloudPlatform/pgadapter/issues/1403)) ([542f2dc](https://github.com/GoogleCloudPlatform/pgadapter/commit/542f2dc99ba72242c217078263a1e1dccc4068ad))
* support bit-reversed sequences in gorm ([#1397](https://github.com/GoogleCloudPlatform/pgadapter/issues/1397)) ([77d2307](https://github.com/GoogleCloudPlatform/pgadapter/commit/77d2307c322bef537a984628e1abbc83ca154f7d))
* support knex ([#1169](https://github.com/GoogleCloudPlatform/pgadapter/issues/1169)) ([0a78cf6](https://github.com/GoogleCloudPlatform/pgadapter/commit/0a78cf628bd18c60a6678404f4bc6f0c796cf898))
* support large batches in gorm ([#1399](https://github.com/GoogleCloudPlatform/pgadapter/issues/1399)) ([24e49de](https://github.com/GoogleCloudPlatform/pgadapter/commit/24e49de987abd154ea017f24a794066c9b204410))


### Bug Fixes

* allow space between timestamp and timezone ([#1390](https://github.com/GoogleCloudPlatform/pgadapter/issues/1390)) ([9e91973](https://github.com/GoogleCloudPlatform/pgadapter/commit/9e91973a3641e3209ed5479f6c77bf67ec77a954))
* limit the num bytes to write to length ([#1388](https://github.com/GoogleCloudPlatform/pgadapter/issues/1388)) ([028fdd4](https://github.com/GoogleCloudPlatform/pgadapter/commit/028fdd46d2069bec24b899d9167ed9c02c86d0d1))


### Performance Improvements

* support virtual threads on Java 21 and higher ([#1406](https://github.com/GoogleCloudPlatform/pgadapter/issues/1406)) ([ebdf1dd](https://github.com/GoogleCloudPlatform/pgadapter/commit/ebdf1dd624265d237ecf5f1d738dd9862f5ae49d))


### Dependencies

* bouncycastle was moved to new Maven coordinates ([#1420](https://github.com/GoogleCloudPlatform/pgadapter/issues/1420)) ([53b95ce](https://github.com/GoogleCloudPlatform/pgadapter/commit/53b95ce27ba88641ea271a857f803bb330526d83))
* bump Docker images to Java 21 ([#1410](https://github.com/GoogleCloudPlatform/pgadapter/issues/1410)) ([b696e7f](https://github.com/GoogleCloudPlatform/pgadapter/commit/b696e7f8a24386fd38fe2e407b0c6818f8021541))


### Documentation

* add sample for using bit-reversed sequence with plain Hibernate ([#1416](https://github.com/GoogleCloudPlatform/pgadapter/issues/1416)) ([ca23785](https://github.com/GoogleCloudPlatform/pgadapter/commit/ca237859625be7a8626cd79416af59baef16a0b7))
* remove samples for Cloud Run that embed PGAdapter in Docker image ([#1415](https://github.com/GoogleCloudPlatform/pgadapter/issues/1415)) ([d60dc31](https://github.com/GoogleCloudPlatform/pgadapter/commit/d60dc317fbce6cecace1342d9e0bbaa268f131f9))

## [0.29.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.29.0...v0.29.1) (2024-02-09)


### Bug Fixes

* add libs to Docker build with emulator ([#1375](https://github.com/GoogleCloudPlatform/pgadapter/issues/1375)) ([e74edfd](https://github.com/GoogleCloudPlatform/pgadapter/commit/e74edfd3ab138b029d66b517619a7d2cce9c7022))


### Performance Improvements

* reduce memory consumption during conversion of JSONB in binary format ([#1346](https://github.com/GoogleCloudPlatform/pgadapter/issues/1346)) ([adbbd2b](https://github.com/GoogleCloudPlatform/pgadapter/commit/adbbd2b2e0999fb1fb500ecf1f4560e8e678f6f4))

## [0.29.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.28.0...v0.29.0) (2024-02-06)


### Features

* support Emulator in the Go wrapper ([#1331](https://github.com/GoogleCloudPlatform/pgadapter/issues/1331)) ([9e106d5](https://github.com/GoogleCloudPlatform/pgadapter/commit/9e106d587e1f65495083b8fb096bd5c5a238272c))


### Bug Fixes

* do not allocate statement name if it fails ([#1252](https://github.com/GoogleCloudPlatform/pgadapter/issues/1252)) ([d438ce5](https://github.com/GoogleCloudPlatform/pgadapter/commit/d438ce52a2359c8bd312f3e0d71e6762db11d824))


### Documentation

* update JDBC sample to use emulator ([#1330](https://github.com/GoogleCloudPlatform/pgadapter/issues/1330)) ([0af129d](https://github.com/GoogleCloudPlatform/pgadapter/commit/0af129d7e94251559358ec69b8e9e48bef53316b))
* use emulator for the pgx sample ([#1332](https://github.com/GoogleCloudPlatform/pgadapter/issues/1332)) ([5f20e56](https://github.com/GoogleCloudPlatform/pgadapter/commit/5f20e5657ff6b1bf07be8213233eccb7a8980916))

## [0.28.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.27.1...v0.28.0) (2023-12-29)


### Features

* translate integrity constraint error codes ([#1266](https://github.com/GoogleCloudPlatform/pgadapter/issues/1266)) ([9d1ed5a](https://github.com/GoogleCloudPlatform/pgadapter/commit/9d1ed5a59b4198d31c39eefd30f7db15c15f4747))


### Bug Fixes

* support pg_database ([#1308](https://github.com/GoogleCloudPlatform/pgadapter/issues/1308)) ([55d1aac](https://github.com/GoogleCloudPlatform/pgadapter/commit/55d1aacc8fd2ae80051e17c1fee0881f06285576))
* update ycsb service account ([#1309](https://github.com/GoogleCloudPlatform/pgadapter/issues/1309)) ([771028b](https://github.com/GoogleCloudPlatform/pgadapter/commit/771028bc5498e88fcc4c4582d8647d1dac6fc2f5))
* use array lower bound 1 by default ([#1302](https://github.com/GoogleCloudPlatform/pgadapter/issues/1302)) ([78e01fa](https://github.com/GoogleCloudPlatform/pgadapter/commit/78e01fa93980923801b9688df93fda0ea8f495b0))


### Dependencies

* use libraries bom for google cloud deps ([#1301](https://github.com/GoogleCloudPlatform/pgadapter/issues/1301)) ([4575b29](https://github.com/GoogleCloudPlatform/pgadapter/commit/4575b29943de6ba3f9765a817dc5dcf33a438075))


### Documentation

* simplify the in-process sample ([#1273](https://github.com/GoogleCloudPlatform/pgadapter/issues/1273)) ([f8b3c9f](https://github.com/GoogleCloudPlatform/pgadapter/commit/f8b3c9f15339c9172caf614763f55944e095d201))

## [0.27.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.27.0...v0.27.1) (2023-11-23)


### Bug Fixes

* re-instante public constructor for ProxyServer ([#1216](https://github.com/GoogleCloudPlatform/pgadapter/issues/1216)) ([a8ce633](https://github.com/GoogleCloudPlatform/pgadapter/commit/a8ce6332aeefa4612956bab629721eeb87315d87))

## [0.27.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.26.0...v0.27.0) (2023-11-23)


### Features

* add OpenTelemetry tracing ([#1182](https://github.com/GoogleCloudPlatform/pgadapter/issues/1182)) ([26217a3](https://github.com/GoogleCloudPlatform/pgadapter/commit/26217a3a937d3004813ceeedcca8fe452d1a70d9))
* support PostgreSQL JDBC 42.7.0 ([#1208](https://github.com/GoogleCloudPlatform/pgadapter/issues/1208)) ([425d530](https://github.com/GoogleCloudPlatform/pgadapter/commit/425d530d6a54ee18bdef672ae067d5a9772da972))


### Documentation

* document emulator usage ([#1178](https://github.com/GoogleCloudPlatform/pgadapter/issues/1178)) ([539ea4d](https://github.com/GoogleCloudPlatform/pgadapter/commit/539ea4d44d0de70078ee0f8eab93ac5642362a41))

## [0.26.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.25.0...v0.26.0) (2023-11-07)


### Features

* improve logging of statement execution ([#1167](https://github.com/GoogleCloudPlatform/pgadapter/issues/1167)) ([ddc4c04](https://github.com/GoogleCloudPlatform/pgadapter/commit/ddc4c04c56a1bba71af7c526971bfff0ff39e751))


### Bug Fixes

* replace fully-qualified column names ([#1158](https://github.com/GoogleCloudPlatform/pgadapter/issues/1158)) ([aeb2e32](https://github.com/GoogleCloudPlatform/pgadapter/commit/aeb2e32e073b392af6c90cb1cdd0980f8723e243))


### Dependencies

* update github.com/googlecloudplatform/pgadapter/wrappers/golang digest to d26b77d ([#1091](https://github.com/GoogleCloudPlatform/pgadapter/issues/1091)) ([f6c22b6](https://github.com/GoogleCloudPlatform/pgadapter/commit/f6c22b69ae38d7e06e710192154c8278744cd308))
* update ycsb dependencies ([#1142](https://github.com/GoogleCloudPlatform/pgadapter/issues/1142)) ([7bb4af0](https://github.com/GoogleCloudPlatform/pgadapter/commit/7bb4af0fee07c8e38aac2ad9d58a41cbf70e9b69))


### Documentation

* add protocol hint to pgbench documentation ([#1140](https://github.com/GoogleCloudPlatform/pgadapter/issues/1140)) ([05bed1e](https://github.com/GoogleCloudPlatform/pgadapter/commit/05bed1e486fe78f2bfc85722a01ef94c2f2d2bac))

## [0.25.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.24.0...v0.25.0) (2023-10-14)


### Features

* support drop cascade ([#841](https://github.com/GoogleCloudPlatform/pgadapter/issues/841)) ([0ffa659](https://github.com/GoogleCloudPlatform/pgadapter/commit/0ffa6598120b914737519b45914cc149cedffc77))


### Bug Fixes

* accept statements with only comments ([#1122](https://github.com/GoogleCloudPlatform/pgadapter/issues/1122)) ([7439aac](https://github.com/GoogleCloudPlatform/pgadapter/commit/7439aacd7b93d3f40015ea41744ce2d71c23ebb7))


### Dependencies

* bump spanner to 6.51.0 ([#1121](https://github.com/GoogleCloudPlatform/pgadapter/issues/1121)) ([e2e326b](https://github.com/GoogleCloudPlatform/pgadapter/commit/e2e326b9932c30a4eef6c531a80365765659cb9c))
* update dependency npgsql to v7.0.6 ([#1094](https://github.com/GoogleCloudPlatform/pgadapter/issues/1094)) ([56b1894](https://github.com/GoogleCloudPlatform/pgadapter/commit/56b18945a4e62b8fecf389a5191bc14ebd38bac8))
* update dependency sqlalchemy to v1.4.49 ([#1095](https://github.com/GoogleCloudPlatform/pgadapter/issues/1095)) ([de12090](https://github.com/GoogleCloudPlatform/pgadapter/commit/de12090539bb85a8a43ae542d23c6d337f3897a0))
* update module github.com/docker/docker to v23.0.7+incompatible ([#1096](https://github.com/GoogleCloudPlatform/pgadapter/issues/1096)) ([4384ffb](https://github.com/GoogleCloudPlatform/pgadapter/commit/4384ffbd690268af4de3756c8a02c7b84062d1ab))
* update spring-boot.version to v2.7.16 ([#1098](https://github.com/GoogleCloudPlatform/pgadapter/issues/1098)) ([9c0b9d9](https://github.com/GoogleCloudPlatform/pgadapter/commit/9c0b9d9b61ea55be6c5ff3b4814b08ffed2c0694))

## [0.24.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.23.1...v0.24.0) (2023-09-21)


### Features

* translate Aborted to SerializationFailure errors ([#1045](https://github.com/GoogleCloudPlatform/pgadapter/issues/1045)) ([e5fce8b](https://github.com/GoogleCloudPlatform/pgadapter/commit/e5fce8b9fecd6efbbdf97c406a96504a30643a81))


### Dependencies

* bump Spanner to 6.47.0 ([#1047](https://github.com/GoogleCloudPlatform/pgadapter/issues/1047)) ([bccdcbc](https://github.com/GoogleCloudPlatform/pgadapter/commit/bccdcbc77bd318579cf60773c3d3f2351b8aa742))


### Documentation

* add missing 's' in fully qualified name in error message ([54182d0](https://github.com/GoogleCloudPlatform/pgadapter/commit/54182d0a1a1c3f2aa8425ecc36ead103d2ba3ec5))
* add missing 's' in fully qualified name in error message ([#1058](https://github.com/GoogleCloudPlatform/pgadapter/issues/1058)) ([3db97ba](https://github.com/GoogleCloudPlatform/pgadapter/commit/3db97ba5115be7ed0a87eb8af3cd59e69eb966f0))
* update Hibernate sample to Hibernate 6.2 ([#1020](https://github.com/GoogleCloudPlatform/pgadapter/issues/1020)) ([71dc0ef](https://github.com/GoogleCloudPlatform/pgadapter/commit/71dc0efa00cbbb66b755e839c16498f36e72793d))
* use Go wrapper for gorm sample ([#1055](https://github.com/GoogleCloudPlatform/pgadapter/issues/1055)) ([bc71c8b](https://github.com/GoogleCloudPlatform/pgadapter/commit/bc71c8be1962a1807313b2527255bb40601baa74))

## [0.23.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.23.0...v0.23.1) (2023-09-07)


### Documentation

* include dependencies in sample README ([#1001](https://github.com/GoogleCloudPlatform/pgadapter/issues/1001)) ([d2c5529](https://github.com/GoogleCloudPlatform/pgadapter/commit/d2c55299ff2e5a5d7a57a3266776304129d14109))
* modify pgx sample to use Go PGAdapter wrapper ([#927](https://github.com/GoogleCloudPlatform/pgadapter/issues/927)) ([3514bbc](https://github.com/GoogleCloudPlatform/pgadapter/commit/3514bbc7d618e8c5112e1af7a2288f79a667c0c5))
* use Options Builder for samples ([#918](https://github.com/GoogleCloudPlatform/pgadapter/issues/918)) ([cc982c6](https://github.com/GoogleCloudPlatform/pgadapter/commit/cc982c62453a3a84b966e8b8f8064dbd81f7c73a))


### Dependencies

* bump GitHub Actions gcloud ([#1028](https://github.com/GoogleCloudPlatform/pgadapter/issues/1028)) ([0215ea3](https://github.com/GoogleCloudPlatform/pgadapter/commit/0215ea35854030dfe315fe303eb5d6ab3d47f92d))
* bump Spring Boot to 3.1.3 for Cloud Run sample ([#1023](https://github.com/GoogleCloudPlatform/pgadapter/issues/1023)) ([bc2ed9e](https://github.com/GoogleCloudPlatform/pgadapter/commit/bc2ed9ee911d2616ad6be8b62a14d4f8deb09099))
* import Spanner pom instead of individual deps ([#986](https://github.com/GoogleCloudPlatform/pgadapter/issues/986)) ([be3d7fd](https://github.com/GoogleCloudPlatform/pgadapter/commit/be3d7fd4b052052009182a83af1d6fd0f6d9a3c1))

## [0.23.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.22.1...v0.23.0) (2023-08-10)


### Features

* add Go wrapper for PGAdapter ([#926](https://github.com/GoogleCloudPlatform/pgadapter/issues/926)) ([5bb71b0](https://github.com/GoogleCloudPlatform/pgadapter/commit/5bb71b038325426ac520ee89dae4ae66dba1b305))
* create options builder ([#917](https://github.com/GoogleCloudPlatform/pgadapter/issues/917)) ([3ceec38](https://github.com/GoogleCloudPlatform/pgadapter/commit/3ceec3827768f9a7637e1b9e2c3eeb8906e57e60))


### Documentation

* Cloud Run sidecar with .NET sample app ([#965](https://github.com/GoogleCloudPlatform/pgadapter/issues/965)) ([bae6f23](https://github.com/GoogleCloudPlatform/pgadapter/commit/bae6f23b8862929b14b0abfc98f2e6c2f1f8305a))
* Cloud Run sidecar with Go sample app ([#962](https://github.com/GoogleCloudPlatform/pgadapter/issues/962)) ([03fdbf1](https://github.com/GoogleCloudPlatform/pgadapter/commit/03fdbf172401616401c8de5db5a84910b4b1abdf))
* Cloud Run sidecar with Java app sample ([#959](https://github.com/GoogleCloudPlatform/pgadapter/issues/959)) ([e6603ef](https://github.com/GoogleCloudPlatform/pgadapter/commit/e6603ef73a734ebfdd08a52c11cf3264e645a3f8))
* Cloud Run sidecar with Node.js sample app ([#963](https://github.com/GoogleCloudPlatform/pgadapter/issues/963)) ([dd2d420](https://github.com/GoogleCloudPlatform/pgadapter/commit/dd2d4208e5c77ee740378e976284a048465e6003))
* Cloud Run sidecar with Python sample app ([#964](https://github.com/GoogleCloudPlatform/pgadapter/issues/964)) ([81d0d36](https://github.com/GoogleCloudPlatform/pgadapter/commit/81d0d360d03a597ff7242ce7254013180160d2b3))
* Cloud Run sidecar with Ruby sample app ([#966](https://github.com/GoogleCloudPlatform/pgadapter/issues/966)) ([e9914b3](https://github.com/GoogleCloudPlatform/pgadapter/commit/e9914b39951b4b579853c9a884f758598bcd27c7))
* create generic cloud-run sidecar sample ([#973](https://github.com/GoogleCloudPlatform/pgadapter/issues/973)) ([00ce9b5](https://github.com/GoogleCloudPlatform/pgadapter/commit/00ce9b5d3dd9bb5d7792f6cd03294c518cece232))


### Dependencies

* bump Spanner to 6.45.0 ([#974](https://github.com/GoogleCloudPlatform/pgadapter/issues/974)) ([f39d70f](https://github.com/GoogleCloudPlatform/pgadapter/commit/f39d70fd614a776196f0ecc2ecfe485e39bb4111))
* update dependabot config ([#938](https://github.com/GoogleCloudPlatform/pgadapter/issues/938)) ([16adeab](https://github.com/GoogleCloudPlatform/pgadapter/commit/16adeab4f521f05f4a2524e774f008234f0a4cb1))

## [0.22.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.22.0...v0.22.1) (2023-07-03)


### Documentation

* add sample for Spring Data JPA ([#892](https://github.com/GoogleCloudPlatform/pgadapter/issues/892)) ([abb7f12](https://github.com/GoogleCloudPlatform/pgadapter/commit/abb7f12e233c561a01af8ef075fcad699d279215))
* document mysql import steps ([#872](https://github.com/GoogleCloudPlatform/pgadapter/issues/872)) ([5088c64](https://github.com/GoogleCloudPlatform/pgadapter/commit/5088c64e76027466493fa0f43be4d67495226292))
* emphasize running PGAdapter in-process ([#913](https://github.com/GoogleCloudPlatform/pgadapter/issues/913)) ([fea8f3f](https://github.com/GoogleCloudPlatform/pgadapter/commit/fea8f3f3650fc1084201a0aecc6cc82b6b516d70))

## [0.22.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.21.0...v0.22.0) (2023-07-01)


### Features

* create local PostgreSQL copy and support pg_dump and pg_restore ([#878](https://github.com/GoogleCloudPlatform/pgadapter/issues/878)) ([ff0d5ea](https://github.com/GoogleCloudPlatform/pgadapter/commit/ff0d5ea6507408b8443395b82f2a8729e6c4cec0))
* replace 'for update' clauses with LOCK_SCANNED_RANGES=exclusive hint ([#699](https://github.com/GoogleCloudPlatform/pgadapter/issues/699)) ([08b567c](https://github.com/GoogleCloudPlatform/pgadapter/commit/08b567c8c4163c8c72342c4ff2b1f92b5076a0fc))
* support replacement of DDL statements ([#893](https://github.com/GoogleCloudPlatform/pgadapter/issues/893)) ([7880f1f](https://github.com/GoogleCloudPlatform/pgadapter/commit/7880f1fef8629c532a2726b70a755e1e753c712b))


### Bug Fixes

* remove invalid BootstrapMessage length check ([#910](https://github.com/GoogleCloudPlatform/pgadapter/issues/910)) ([be431a8](https://github.com/GoogleCloudPlatform/pgadapter/commit/be431a8b453f7731522d42e0b775ffbdba2f5346))


### Dependencies

* bump Spanner client to 6.43.0 ([#875](https://github.com/GoogleCloudPlatform/pgadapter/issues/875)) ([08fa0ad](https://github.com/GoogleCloudPlatform/pgadapter/commit/08fa0adff4fb0abffb383ea4602842ac044cee2f))


### Documentation

* add Cloud Run sample for Java ([#883](https://github.com/GoogleCloudPlatform/pgadapter/issues/883)) ([ddbba41](https://github.com/GoogleCloudPlatform/pgadapter/commit/ddbba4114aa84739106bda0b5f260b6ab32c55ce))
* add Cloud Run sample for Python ([#884](https://github.com/GoogleCloudPlatform/pgadapter/issues/884)) ([576a56a](https://github.com/GoogleCloudPlatform/pgadapter/commit/576a56ae5113514a56681abaac2acb6a8f4f1c57))
* add sample for Node.js with Cloud Run ([#885](https://github.com/GoogleCloudPlatform/pgadapter/issues/885)) ([7b03c4e](https://github.com/GoogleCloudPlatform/pgadapter/commit/7b03c4ed512e4ef173c3e4f6750d496f5d5e9c24))
* Go sample equal to the other samples ([#886](https://github.com/GoogleCloudPlatform/pgadapter/issues/886)) ([f7ef506](https://github.com/GoogleCloudPlatform/pgadapter/commit/f7ef5060a6aebf0d1ede1d9da26b3ff036c3f45b))
* update IntelliJ example to allow all databases ([#890](https://github.com/GoogleCloudPlatform/pgadapter/issues/890)) ([62afdf9](https://github.com/GoogleCloudPlatform/pgadapter/commit/62afdf9f479b08b12c2df528480995b6e088bfee))

## [0.21.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.20.0...v0.21.0) (2023-06-07)


### Features

* support cursors and foreign data wrapper ([#797](https://github.com/GoogleCloudPlatform/pgadapter/issues/797)) ([5ee2200](https://github.com/GoogleCloudPlatform/pgadapter/commit/5ee220070257f8abf1106caf1fde1bd779ed25ec))
* support setting the well-known client ([#837](https://github.com/GoogleCloudPlatform/pgadapter/issues/837)) ([045156c](https://github.com/GoogleCloudPlatform/pgadapter/commit/045156cda729975f34c63a084c606470903850ef))


### Documentation

* document how to connect to IntelliJ ([#870](https://github.com/GoogleCloudPlatform/pgadapter/issues/870)) ([35ec68f](https://github.com/GoogleCloudPlatform/pgadapter/commit/35ec68f78598ce45246ffbaa9092caac58070bb3))

## [0.20.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.19.1...v0.20.0) (2023-05-26)


### Features

* add support for Ruby ActiveRecord ([#749](https://github.com/GoogleCloudPlatform/pgadapter/issues/749)) ([442e45c](https://github.com/GoogleCloudPlatform/pgadapter/commit/442e45ceef9e8ac9d834126a3f90d03b75b6821a))
* detect relation not found errors ([#834](https://github.com/GoogleCloudPlatform/pgadapter/issues/834)) ([8d21df7](https://github.com/GoogleCloudPlatform/pgadapter/commit/8d21df7e427fbbbff75ab71cac04adb339eba49e))


### Bug Fixes

* latency benchmark should use random local port ([#843](https://github.com/GoogleCloudPlatform/pgadapter/issues/843)) ([e15711f](https://github.com/GoogleCloudPlatform/pgadapter/commit/e15711f4ef0c80c391b4df70abb349029e6c80cc))

## [0.19.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.19.0...v0.19.1) (2023-05-17)


### Performance Improvements

* add latency comparision test ([#790](https://github.com/GoogleCloudPlatform/pgadapter/issues/790)) ([9be6b09](https://github.com/GoogleCloudPlatform/pgadapter/commit/9be6b09d5e156d7f916d25467ca679e028aa19d1))
* bundle Ready response with the rest ([#806](https://github.com/GoogleCloudPlatform/pgadapter/issues/806)) ([af98023](https://github.com/GoogleCloudPlatform/pgadapter/commit/af98023a7363d6a2bb2bfb356b747f3ed8499e0e))

## [0.19.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.18.4...v0.19.0) (2023-05-05)


### Features

* automatically add LIMIT clause ([#792](https://github.com/GoogleCloudPlatform/pgadapter/issues/792)) ([c382392](https://github.com/GoogleCloudPlatform/pgadapter/commit/c38239213dabab0c17cd1b1ffedfe033607cbcbc))
* emulate pg_class and related tables ([#766](https://github.com/GoogleCloudPlatform/pgadapter/issues/766)) ([e602750](https://github.com/GoogleCloudPlatform/pgadapter/commit/e60275074ca7de8e88d63c6aa7465d90780b9a59))
* support savepoints ([#796](https://github.com/GoogleCloudPlatform/pgadapter/issues/796)) ([570fb96](https://github.com/GoogleCloudPlatform/pgadapter/commit/570fb96b4b42e903000e4e5efd3428bfca90f57d))


### Documentation

* add sample for Cloud Run ([#770](https://github.com/GoogleCloudPlatform/pgadapter/issues/770)) ([751030d](https://github.com/GoogleCloudPlatform/pgadapter/commit/751030db44797ab5a006b6b0ffadc34d66bb16c3))
* document psycopg3 and SQLAlchemy 2.x support ([#801](https://github.com/GoogleCloudPlatform/pgadapter/issues/801)) ([fe73c63](https://github.com/GoogleCloudPlatform/pgadapter/commit/fe73c6353cfa539e5f73945fa74e0d9df5d5b02e))

## [0.18.4](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.18.3...v0.18.4) (2023-04-15)


### Documentation

* recommend psycopg3 over psycopg2 ([#742](https://github.com/GoogleCloudPlatform/pgadapter/issues/742)) ([8aec410](https://github.com/GoogleCloudPlatform/pgadapter/commit/8aec410c4ff14df6b3787d0988bb868665cdf1b7))


### Dependencies

* bump JDBC driver to 42.6.0 ([#747](https://github.com/GoogleCloudPlatform/pgadapter/issues/747)) ([9bdb5f4](https://github.com/GoogleCloudPlatform/pgadapter/commit/9bdb5f477ffcfd3861e43305bcf8c229a437b7e8))
* bump Spanner client lib version to 6.40.0 ([#781](https://github.com/GoogleCloudPlatform/pgadapter/issues/781)) ([419c833](https://github.com/GoogleCloudPlatform/pgadapter/commit/419c8336eed28115520e4cc03d28b2ff60415138))
* pin the SQLAlchemy 2.x version used for tests ([#759](https://github.com/GoogleCloudPlatform/pgadapter/issues/759)) ([2a78f4c](https://github.com/GoogleCloudPlatform/pgadapter/commit/2a78f4cd0b72f0f7432081816a3b6173ed65c613))

## [0.18.3](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.18.2...v0.18.3) (2023-03-20)


### Bug Fixes

* close SpannerPool at shutdown ([#734](https://github.com/GoogleCloudPlatform/pgadapter/issues/734)) ([d67a75f](https://github.com/GoogleCloudPlatform/pgadapter/commit/d67a75f4be16db958cfb268277649a62d5e99e7c))


### Documentation

* add psycopg3 sample with embedded PGAdapter ([#733](https://github.com/GoogleCloudPlatform/pgadapter/issues/733)) ([0250b21](https://github.com/GoogleCloudPlatform/pgadapter/commit/0250b2165247bbc5ee8a48874822c11f264a9a86))
* add sample application for JDBC ([#592](https://github.com/GoogleCloudPlatform/pgadapter/issues/592)) ([c1a5635](https://github.com/GoogleCloudPlatform/pgadapter/commit/c1a56358030c97b20d6ff3d774be26b755f837bf))
* document SQLAlchemy 2.x experimental support ([#728](https://github.com/GoogleCloudPlatform/pgadapter/issues/728)) ([6a40e9e](https://github.com/GoogleCloudPlatform/pgadapter/commit/6a40e9e5a54d161dc44280c4c6a6a7d09cbb2490))
* pgx sample using embedded PGAdapter ([#732](https://github.com/GoogleCloudPlatform/pgadapter/issues/732)) ([7c95781](https://github.com/GoogleCloudPlatform/pgadapter/commit/7c9578165aebade978054aedd440db4349080d23))

## [0.18.2](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.18.1...v0.18.2) (2023-03-17)


### Bug Fixes

* add support for getting arrays using npgsql ([#722](https://github.com/GoogleCloudPlatform/pgadapter/issues/722)) ([6988ad1](https://github.com/GoogleCloudPlatform/pgadapter/commit/6988ad17c06a9e9197fd71922492ff76eafb280b))


### Documentation

* add documentation and sample for GKE sidecar proxy ([#718](https://github.com/GoogleCloudPlatform/pgadapter/issues/718)) ([f4246bd](https://github.com/GoogleCloudPlatform/pgadapter/commit/f4246bd81e66ffe7f20c9f78f5e59302635de5ed)), closes [#701](https://github.com/GoogleCloudPlatform/pgadapter/issues/701)

## [0.18.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.18.0...v0.18.1) (2023-03-09)


### Bug Fixes

* the JDBC reWriteBatchedInserts=true option could cause errors in DML batches ([#713](https://github.com/GoogleCloudPlatform/pgadapter/issues/713)) ([36bff88](https://github.com/GoogleCloudPlatform/pgadapter/commit/36bff88c6476d4e0f3b8f20797182eab56646143))

## [0.18.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.17.0...v0.18.0) (2023-03-06)


### Features

* support arrays in copy operations ([#690](https://github.com/GoogleCloudPlatform/pgadapter/issues/690)) ([ac624e5](https://github.com/GoogleCloudPlatform/pgadapter/commit/ac624e51e7949362f85b11e5f79199f13d5e140c))


### Bug Fixes

* add pg_sequence and pg_sequences views ([#700](https://github.com/GoogleCloudPlatform/pgadapter/issues/700)) ([2ad990a](https://github.com/GoogleCloudPlatform/pgadapter/commit/2ad990a86def4b01aaf97475d5325e0f6c3266c9))
* allow unquoted text values in arrays ([#706](https://github.com/GoogleCloudPlatform/pgadapter/issues/706)) ([b09f540](https://github.com/GoogleCloudPlatform/pgadapter/commit/b09f540eff12e76d9d2059537d27a335efb77c65))
* support information_schema.sequences ([#708](https://github.com/GoogleCloudPlatform/pgadapter/issues/708)) ([c043c46](https://github.com/GoogleCloudPlatform/pgadapter/commit/c043c46f811a68bc537c13ff35840d413b0d786b)), closes [#705](https://github.com/GoogleCloudPlatform/pgadapter/issues/705)
* timestamp arrays should use timestamptz_array OID ([#691](https://github.com/GoogleCloudPlatform/pgadapter/issues/691)) ([52cac8c](https://github.com/GoogleCloudPlatform/pgadapter/commit/52cac8ceb00af6d5b88476146c24beb2a3b6b34d))

## [0.17.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.16.0...v0.17.0) (2023-02-24)


### Features

* allow timestamp param values in brackets ([#661](https://github.com/GoogleCloudPlatform/pgadapter/issues/661)) ([f84235f](https://github.com/GoogleCloudPlatform/pgadapter/commit/f84235ff5bfc372a1d11c191b1743ac3a29ba41b))
* support ARRAY typed query parameters ([#653](https://github.com/GoogleCloudPlatform/pgadapter/issues/653)) ([104c200](https://github.com/GoogleCloudPlatform/pgadapter/commit/104c20035c93060352f4832d4d2e6be876fa11c7))


### Bug Fixes

* support 'localtime' as a timezone ([#625](https://github.com/GoogleCloudPlatform/pgadapter/issues/625)) ([f9973f1](https://github.com/GoogleCloudPlatform/pgadapter/commit/f9973f1e51946b6635b224bd8ebdeb9099cb53ce))


### Performance Improvements

* do not create a transaction for describe+execute ([#660](https://github.com/GoogleCloudPlatform/pgadapter/issues/660)) ([e3856ce](https://github.com/GoogleCloudPlatform/pgadapter/commit/e3856ce0e8963cf16cb84ce48e79a3ea39aebd0c))


### Documentation

* add example for using stale reads ([#643](https://github.com/GoogleCloudPlatform/pgadapter/issues/643)) ([a8022c2](https://github.com/GoogleCloudPlatform/pgadapter/commit/a8022c2063a102bf0c0471f399ed610c797301de))
* added jsonb in the sample ([#647](https://github.com/GoogleCloudPlatform/pgadapter/issues/647)) ([01785fa](https://github.com/GoogleCloudPlatform/pgadapter/commit/01785fabd5d556da7237cf9d368cbd168eeea690))
* emphasize that internal debugging is for internal use ([#612](https://github.com/GoogleCloudPlatform/pgadapter/issues/612)) ([005b7f0](https://github.com/GoogleCloudPlatform/pgadapter/commit/005b7f049a69d86986501d8688c8cd2af7a6de49))


### Dependencies

* add dependabot for ecosystem tests and samples ([#675](https://github.com/GoogleCloudPlatform/pgadapter/issues/675)) ([3a6ee4e](https://github.com/GoogleCloudPlatform/pgadapter/commit/3a6ee4e5733e03c0db8c6b968b778224b935b380))
* bump node-postgres to 8.9.0 ([#663](https://github.com/GoogleCloudPlatform/pgadapter/issues/663)) ([ee436f0](https://github.com/GoogleCloudPlatform/pgadapter/commit/ee436f074f3d8aa28244deab9fd375de47b37b45))
* setup dependabot for npgsql tests ([#668](https://github.com/GoogleCloudPlatform/pgadapter/issues/668)) ([e85d38c](https://github.com/GoogleCloudPlatform/pgadapter/commit/e85d38c881c38bb0faa8d41c0778f1f53dd92b73))

## [0.16.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.15.0...v0.16.0) (2023-02-05)


### Features

* allow unsupported OIDs as param types ([#604](https://github.com/GoogleCloudPlatform/pgadapter/issues/604)) ([5e9f95a](https://github.com/GoogleCloudPlatform/pgadapter/commit/5e9f95a720f1648236b39167b227cc70bd40e323))
* make table and function replacements client-aware ([#605](https://github.com/GoogleCloudPlatform/pgadapter/issues/605)) ([ad49e99](https://github.com/GoogleCloudPlatform/pgadapter/commit/ad49e990298d0e91736d4f5afe581d2f1411b5ca))


### Bug Fixes

* binary copy header should be included in first data message ([#609](https://github.com/GoogleCloudPlatform/pgadapter/issues/609)) ([2fbf89e](https://github.com/GoogleCloudPlatform/pgadapter/commit/2fbf89e6a6b3ba0b66f126abf019e386e9276d4c))
* copy to for a query would fail with a column list ([#616](https://github.com/GoogleCloudPlatform/pgadapter/issues/616)) ([16f030e](https://github.com/GoogleCloudPlatform/pgadapter/commit/16f030e3f6b93ae0a243b6c495b0c906403c5e16))
* CopyResponse did not return correct column format ([#633](https://github.com/GoogleCloudPlatform/pgadapter/issues/633)) ([dc0d482](https://github.com/GoogleCloudPlatform/pgadapter/commit/dc0d482ffb61d1857a3f49fc424a07d72886b460))
* csv copy header was repeated for each row ([#619](https://github.com/GoogleCloudPlatform/pgadapter/issues/619)) ([622c49a](https://github.com/GoogleCloudPlatform/pgadapter/commit/622c49a02cf2a865874764f44a77b96539382be0))
* empty copy from stdin statements could be unresponsive ([#617](https://github.com/GoogleCloudPlatform/pgadapter/issues/617)) ([c576124](https://github.com/GoogleCloudPlatform/pgadapter/commit/c576124e40ad7f07ee0d1e2f3090886896c70dc3))
* empty partitions could skip binary copy header ([#615](https://github.com/GoogleCloudPlatform/pgadapter/issues/615)) ([e7dd650](https://github.com/GoogleCloudPlatform/pgadapter/commit/e7dd6508015ed45147af59c25f95e18628461d85))
* show statements failed in pgx ([#629](https://github.com/GoogleCloudPlatform/pgadapter/issues/629)) ([734f521](https://github.com/GoogleCloudPlatform/pgadapter/commit/734f52176f75e4ccb0b8bddc96eae49ace9ab19e))
* support end-of-data record in COPY ([#602](https://github.com/GoogleCloudPlatform/pgadapter/issues/602)) ([8b705e8](https://github.com/GoogleCloudPlatform/pgadapter/commit/8b705e8f917035cbabe9e6751008e93692355158))


### Dependencies

* update Spanner client to 6.35.1 ([#607](https://github.com/GoogleCloudPlatform/pgadapter/issues/607)) ([0c607c7](https://github.com/GoogleCloudPlatform/pgadapter/commit/0c607c7c1bce48139f28688a5d7f1e202d839860))


### Documentation

* document pgbench usage ([#603](https://github.com/GoogleCloudPlatform/pgadapter/issues/603)) ([5a62bf6](https://github.com/GoogleCloudPlatform/pgadapter/commit/5a62bf64c56a976625e2c707b6d049e593cddc96))
* document unix domain sockets with Docker ([#622](https://github.com/GoogleCloudPlatform/pgadapter/issues/622)) ([e4e41f7](https://github.com/GoogleCloudPlatform/pgadapter/commit/e4e41f70e5ad23d8e7d6f2a1bc1851458466bbb6))

## [0.15.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.14.1...v0.15.0) (2023-01-18)


### Features

* allow decimal values to be used for int params ([#564](https://github.com/GoogleCloudPlatform/pgadapter/issues/564)) ([3aecf34](https://github.com/GoogleCloudPlatform/pgadapter/commit/3aecf34aa1500fc593c3a33d4eab132f24fbb2d8))
* auto-detect npgsql ([#559](https://github.com/GoogleCloudPlatform/pgadapter/issues/559)) ([e1e8526](https://github.com/GoogleCloudPlatform/pgadapter/commit/e1e85267b8b77a853916b911b37548bd3f5f97c7))
* support more timestamp values for query parameters ([#565](https://github.com/GoogleCloudPlatform/pgadapter/issues/565)) ([5905213](https://github.com/GoogleCloudPlatform/pgadapter/commit/5905213b9b662b1f1bb8afc1ef0ca4a5cdaf7097))
* support TRUNCATE ([#533](https://github.com/GoogleCloudPlatform/pgadapter/issues/533)) ([293aaaa](https://github.com/GoogleCloudPlatform/pgadapter/commit/293aaaa9f0db0dada42e246708678501aba39ea6))
* support VACUUM as a no-op ([#532](https://github.com/GoogleCloudPlatform/pgadapter/issues/532)) ([20dc062](https://github.com/GoogleCloudPlatform/pgadapter/commit/20dc062c1d10b69377c5d7da26c2bf12dc7fd00d))


### Bug Fixes

* potential session leak when using prepared statements in autocommit mode ([#591](https://github.com/GoogleCloudPlatform/pgadapter/issues/591)) ([185dcd5](https://github.com/GoogleCloudPlatform/pgadapter/commit/185dcd5635d0a8c042cc8975ccb58b331dd29eb7))
* support ([#586](https://github.com/GoogleCloudPlatform/pgadapter/issues/586)) ([c91fe66](https://github.com/GoogleCloudPlatform/pgadapter/commit/c91fe6651d7040680211db4b2fecea6c26a69e31)), closes [#581](https://github.com/GoogleCloudPlatform/pgadapter/issues/581)


### Performance Improvements

* keep converted bytes ([#584](https://github.com/GoogleCloudPlatform/pgadapter/issues/584)) ([1486bc4](https://github.com/GoogleCloudPlatform/pgadapter/commit/1486bc4a57ac70155f67823ca82063956122d772))

## [0.14.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.14.0...v0.14.1) (2023-01-06)


### Bug Fixes

* enable gracefully stopping Docker container ([#554](https://github.com/GoogleCloudPlatform/pgadapter/issues/554)) ([80e9c8b](https://github.com/GoogleCloudPlatform/pgadapter/commit/80e9c8b988a168b30d44335b5de3c09e57c18d76))


### Performance Improvements

* reduce conversion time for timestamptz, date and bytea ([#572](https://github.com/GoogleCloudPlatform/pgadapter/issues/572)) ([bb73990](https://github.com/GoogleCloudPlatform/pgadapter/commit/bb739901bd875a7c706377dc065c1936db56e5d7))

## [0.14.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.13.1...v0.14.0) (2022-12-16)


### Features

* support Connection API options in connection string ([#542](https://github.com/GoogleCloudPlatform/pgadapter/issues/542)) ([6247412](https://github.com/GoogleCloudPlatform/pgadapter/commit/6247412b4b0d578f638d763c6470c44db9ddf246))
* support force_autocommit ([#541](https://github.com/GoogleCloudPlatform/pgadapter/issues/541)) ([52fba80](https://github.com/GoogleCloudPlatform/pgadapter/commit/52fba801f20c2bcdc1fa9e78e29bd3d25b18eeb0))


### Documentation

* added sample application model for Django ([#539](https://github.com/GoogleCloudPlatform/pgadapter/issues/539)) ([72b173a](https://github.com/GoogleCloudPlatform/pgadapter/commit/72b173af7130e24a6e3309535a117aaa90df17d6))

## [0.13.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.13.0...v0.13.1) (2022-12-09)


### Bug Fixes

* allow start ddl batch / run batch in one query string ([#529](https://github.com/GoogleCloudPlatform/pgadapter/issues/529)) ([2ffb290](https://github.com/GoogleCloudPlatform/pgadapter/commit/2ffb290bf661d99bba6648460c25d73b47fc2bf4))
* always return timestamp in microsecond precision ([#513](https://github.com/GoogleCloudPlatform/pgadapter/issues/513)) ([e385dd3](https://github.com/GoogleCloudPlatform/pgadapter/commit/e385dd3a7fb2cef2d02616853c283d4869d0f32d))


### Documentation

* document node-postgres support ([#489](https://github.com/GoogleCloudPlatform/pgadapter/issues/489)) ([61c6459](https://github.com/GoogleCloudPlatform/pgadapter/commit/61c64593f08899653cfdbddab8b8b5705cd2be0c))

## [0.13.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.12.0...v0.13.0) (2022-12-07)


### Features

* accept UUID as a parameter value ([#518](https://github.com/GoogleCloudPlatform/pgadapter/issues/518)) ([46941ab](https://github.com/GoogleCloudPlatform/pgadapter/commit/46941ab318e4269061336e2ecb95a4402cf2a5e5))
* support 'select version()' and similar ([#495](https://github.com/GoogleCloudPlatform/pgadapter/issues/495)) ([fbd16ec](https://github.com/GoogleCloudPlatform/pgadapter/commit/fbd16ecd44d12ffb65b85555d2ddef0cc533b4be))
* Support Describe message for DDL statements and other no-result statements ([#501](https://github.com/GoogleCloudPlatform/pgadapter/issues/501)) ([cb616d8](https://github.com/GoogleCloudPlatform/pgadapter/commit/cb616d8f64c6aabe0422020d7ce2bc90734ff837))
* support DML RETURNING clause ([#498](https://github.com/GoogleCloudPlatform/pgadapter/issues/498)) ([c1d7e4e](https://github.com/GoogleCloudPlatform/pgadapter/commit/c1d7e4eff240449245f223bc17793f393cafea2f))
* support more than 50 query parameters ([#527](https://github.com/GoogleCloudPlatform/pgadapter/issues/527)) ([9fca9ba](https://github.com/GoogleCloudPlatform/pgadapter/commit/9fca9ba487515d63b586bb4ed6329f2d84d98996))
* use session timezone to format timestamps ([#470](https://github.com/GoogleCloudPlatform/pgadapter/issues/470)) ([d84564d](https://github.com/GoogleCloudPlatform/pgadapter/commit/d84564dc45a4259c3b8246d05c66a2645cb92f2d))


### Bug Fixes

* client side results were not returned ([#493](https://github.com/GoogleCloudPlatform/pgadapter/issues/493)) ([5e9e85e](https://github.com/GoogleCloudPlatform/pgadapter/commit/5e9e85e72b7d51bb6426ad963521fb3e24fa36bb))
* pg_catalog tables were not replaced for information_schema queries ([#494](https://github.com/GoogleCloudPlatform/pgadapter/issues/494)) ([e1f02fe](https://github.com/GoogleCloudPlatform/pgadapter/commit/e1f02fed232c09c96adb426b9f8ce91d61c6659d))


### Documentation

* [WIP] Hibernate sample ([#373](https://github.com/GoogleCloudPlatform/pgadapter/issues/373)) ([7125c91](https://github.com/GoogleCloudPlatform/pgadapter/commit/7125c9110eab429ea311676445c71308c1018aac))
* document Liquibase Pilot Support ([#485](https://github.com/GoogleCloudPlatform/pgadapter/issues/485)) ([745089f](https://github.com/GoogleCloudPlatform/pgadapter/commit/745089f8d7f6df2401eb0fb15cca80c85dc18437))
* document Support for gorm ([#469](https://github.com/GoogleCloudPlatform/pgadapter/issues/469)) ([0b962af](https://github.com/GoogleCloudPlatform/pgadapter/commit/0b962af9f0037b7fb86225ed0b3f89c072bf7bcf))
* remove limitation for RETURNING and generated columns for gorm ([#526](https://github.com/GoogleCloudPlatform/pgadapter/issues/526)) ([0420e99](https://github.com/GoogleCloudPlatform/pgadapter/commit/0420e997fb1c334bd08ee2507ca73ad11426e370))

## [0.12.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.11.0...v0.12.0) (2022-11-02)


### Features

* reimplement COPY parser to support more options and legacy syntax ([#410](https://github.com/GoogleCloudPlatform/pgadapter/issues/410)) ([b8a38dd](https://github.com/GoogleCloudPlatform/pgadapter/commit/b8a38ddf5236222c458b24658dc4d1c75bcc9c19))
* replace sequences query with empty table ([#366](https://github.com/GoogleCloudPlatform/pgadapter/issues/366)) ([170dc7c](https://github.com/GoogleCloudPlatform/pgadapter/commit/170dc7ced61d355fa8ae50e40608f271be056ae7))


### Bug Fixes

* DDL batch errors halfway were not propagated ([#444](https://github.com/GoogleCloudPlatform/pgadapter/issues/444)) ([fc6efa4](https://github.com/GoogleCloudPlatform/pgadapter/commit/fc6efa409e9060a66a6e6fd099c4add92ef3a968)), closes [#443](https://github.com/GoogleCloudPlatform/pgadapter/issues/443)

## [0.11.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.10.1...v0.11.0) (2022-10-28)


### Features

* add setting for copy_commit_priority ([#426](https://github.com/GoogleCloudPlatform/pgadapter/issues/426)) ([6d23184](https://github.com/GoogleCloudPlatform/pgadapter/commit/6d231847491895b1c5261eff97e810faa68c4e47))


### Bug Fixes

* close buffers used by Converter after use ([#434](https://github.com/GoogleCloudPlatform/pgadapter/issues/434)) ([4b0b500](https://github.com/GoogleCloudPlatform/pgadapter/commit/4b0b500e977aefa28e30a2b3bf2b9dba5729b757))
* memory leak caused by ConnectionHandler not removed from map ([#437](https://github.com/GoogleCloudPlatform/pgadapter/issues/437)) ([6ef7240](https://github.com/GoogleCloudPlatform/pgadapter/commit/6ef7240017e7fee9786132c92ac32b3aecc6f63c))
* use copy commit timeout for all RPCs ([#427](https://github.com/GoogleCloudPlatform/pgadapter/issues/427)) ([0381a0c](https://github.com/GoogleCloudPlatform/pgadapter/commit/0381a0c03aa8b2791ac7dc55c7b34e6b759b8192))


### Documentation

* document support for psycopg2 ([#395](https://github.com/GoogleCloudPlatform/pgadapter/issues/395)) ([676ddcd](https://github.com/GoogleCloudPlatform/pgadapter/commit/676ddcd05cd1f3cefca08dfec252d7c2771baa67))

## [0.10.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.10.0...v0.10.1) (2022-10-21)


### Performance Improvements

* use low latency TCP options ([#414](https://github.com/GoogleCloudPlatform/pgadapter/issues/414)) ([684506a](https://github.com/GoogleCloudPlatform/pgadapter/commit/684506a53a2747e843d7bbcb69a2f7e95f2413db))


### Documentation

* add FAQ entries for Docker ([#411](https://github.com/GoogleCloudPlatform/pgadapter/issues/411)) ([cdfb76a](https://github.com/GoogleCloudPlatform/pgadapter/commit/cdfb76ac0566141d2cf5c9a4d28df7edf7b25b26))

## [0.10.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.9.0...v0.10.0) (2022-10-14)


### Features

* add experimental support for node-postgres ([#362](https://github.com/GoogleCloudPlatform/pgadapter/issues/362)) ([9e3e952](https://github.com/GoogleCloudPlatform/pgadapter/commit/9e3e95284a34516967f2acdd2acfeb94ab50a2ea))
* added support for Django specific statement ([#382](https://github.com/GoogleCloudPlatform/pgadapter/issues/382)) ([1137ed1](https://github.com/GoogleCloudPlatform/pgadapter/commit/1137ed16725e7735da72573124244e05503bb19e))


### Performance Improvements

* copy binary can write directly to a byte buffer ([#385](https://github.com/GoogleCloudPlatform/pgadapter/issues/385)) ([18ddbdd](https://github.com/GoogleCloudPlatform/pgadapter/commit/18ddbddcbf2e66f3719fdbb1a43b021c571444e1))
* do not create a new parser for each column ([#383](https://github.com/GoogleCloudPlatform/pgadapter/issues/383)) ([5352f79](https://github.com/GoogleCloudPlatform/pgadapter/commit/5352f791f23689503d3b49c3ee300baa9622ab87))
* write converted values directly to a byte buffer ([#384](https://github.com/GoogleCloudPlatform/pgadapter/issues/384)) ([bec1657](https://github.com/GoogleCloudPlatform/pgadapter/commit/bec1657a43500c1f61126c744ad5b95cdce4c2e7))


### Documentation

* document how to set a statement timeout ([#390](https://github.com/GoogleCloudPlatform/pgadapter/issues/390)) ([2db00f9](https://github.com/GoogleCloudPlatform/pgadapter/commit/2db00f9d1b0c92d7266cb75ad14d924bb42c6543))

## [0.9.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.8.0...v0.9.0) (2022-10-11)


### Features

* added support for the set and show time zone ([#361](https://github.com/GoogleCloudPlatform/pgadapter/issues/361)) ([66e3788](https://github.com/GoogleCloudPlatform/pgadapter/commit/66e3788fa794b2200dca160214a8b2b3a998952e))


### Bug Fixes

* bind to all loopback addresses ([#375](https://github.com/GoogleCloudPlatform/pgadapter/issues/375)) ([82bc566](https://github.com/GoogleCloudPlatform/pgadapter/commit/82bc566bcd7a45c5679bdb33bae693055593edab))
* exceptions without messages could cause NullPointerExceptions ([#381](https://github.com/GoogleCloudPlatform/pgadapter/issues/381)) ([3a9a55e](https://github.com/GoogleCloudPlatform/pgadapter/commit/3a9a55ea3d3f6f7297c4747c3dff3689e42d25fa))


### Documentation

* add docs tag for dependency ([#374](https://github.com/GoogleCloudPlatform/pgadapter/issues/374)) ([a619595](https://github.com/GoogleCloudPlatform/pgadapter/commit/a619595f91946a0c720867adfb4cc474ad6838fd))
* add sample for gorm ([#351](https://github.com/GoogleCloudPlatform/pgadapter/issues/351)) ([840ab53](https://github.com/GoogleCloudPlatform/pgadapter/commit/840ab53b0160c182ddd33808c72a907d101cbaaf))
* document support for JDBC and pgx ([#352](https://github.com/GoogleCloudPlatform/pgadapter/issues/352)) ([847a2e3](https://github.com/GoogleCloudPlatform/pgadapter/commit/847a2e3df2e9dcadbadf752cac0abdd73f91f4d8))

## [0.8.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.7.0...v0.8.0) (2022-09-25)


### Features

* enable SSL connections ([#358](https://github.com/GoogleCloudPlatform/pgadapter/issues/358)) ([c856ce2](https://github.com/GoogleCloudPlatform/pgadapter/commit/c856ce2fa84c1ccb1f4ede31307f7914ea77e002))
* JSONB support ([#328](https://github.com/GoogleCloudPlatform/pgadapter/issues/328)) ([0ec6c7c](https://github.com/GoogleCloudPlatform/pgadapter/commit/0ec6c7c57c9b557fc722c020b299c181aacb7cd0))
* make guess types configurable ([#347](https://github.com/GoogleCloudPlatform/pgadapter/issues/347)) ([87415dd](https://github.com/GoogleCloudPlatform/pgadapter/commit/87415dd08de85ff7906a509e483772308a7c618d))
* support OAuth2 token authentication ([#360](https://github.com/GoogleCloudPlatform/pgadapter/issues/360)) ([0cedf15](https://github.com/GoogleCloudPlatform/pgadapter/commit/0cedf15a95b154bfb2c0f493e932b70035e4738c))


### Bug Fixes

* cancel requests were ignored ([#356](https://github.com/GoogleCloudPlatform/pgadapter/issues/356)) ([2b5add0](https://github.com/GoogleCloudPlatform/pgadapter/commit/2b5add0f37abb8391df85150bb4ac70a82aaa1d9))


### Documentation

* document max 50 parameters limit ([#355](https://github.com/GoogleCloudPlatform/pgadapter/issues/355)) ([7e2fc78](https://github.com/GoogleCloudPlatform/pgadapter/commit/7e2fc78749b0da53da091e9d90003fbd373423d5))

## [0.7.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.6.1...v0.7.0) (2022-09-10)


### Features

* add COPY settings to session state ([#338](https://github.com/GoogleCloudPlatform/pgadapter/issues/338)) ([31a2faf](https://github.com/GoogleCloudPlatform/pgadapter/commit/31a2faff3cf65578dd4a9a129113cd949cf19298))
* add ddl_transaction_mode and replace_pg_catalog_tables settings ([#334](https://github.com/GoogleCloudPlatform/pgadapter/issues/334)) ([aed2b4d](https://github.com/GoogleCloudPlatform/pgadapter/commit/aed2b4d8559e84efc34c981b40e31213fee3d076))
* allow setting the server version number in startup msg ([#336](https://github.com/GoogleCloudPlatform/pgadapter/issues/336)) ([808217e](https://github.com/GoogleCloudPlatform/pgadapter/commit/808217ea8661c7b7aa57d59c9028e12e650bbb1f))
* auto-convert explicit DDL transactions to batches ([#292](https://github.com/GoogleCloudPlatform/pgadapter/issues/292)) ([612fc44](https://github.com/GoogleCloudPlatform/pgadapter/commit/612fc44b1bba70d86a33baae2c78bdf8ceb3dc66))
* execute INFORMATION_SCHEMA in single-use transaction ([#276](https://github.com/GoogleCloudPlatform/pgadapter/issues/276)) ([fa7fe13](https://github.com/GoogleCloudPlatform/pgadapter/commit/fa7fe135161399dc599d9ef5991ab1eb42e5f120))
* ignore named primary keys with correct name ([#286](https://github.com/GoogleCloudPlatform/pgadapter/issues/286)) ([6d90366](https://github.com/GoogleCloudPlatform/pgadapter/commit/6d903666025b43c328ffd3aad31e9cea35a215cb))
* manage session state ([#305](https://github.com/GoogleCloudPlatform/pgadapter/issues/305)) ([9e4d391](https://github.com/GoogleCloudPlatform/pgadapter/commit/9e4d391b2bf5e8c35cf85d67d9586bdda756f2a7))
* PREPARE statement support ([#332](https://github.com/GoogleCloudPlatform/pgadapter/issues/332)) ([980e764](https://github.com/GoogleCloudPlatform/pgadapter/commit/980e764c4febb7a741a96279741b8ca66c89b26c))
* replace pg_catalog tables with common table expressions ([#331](https://github.com/GoogleCloudPlatform/pgadapter/issues/331)) ([4b01399](https://github.com/GoogleCloudPlatform/pgadapter/commit/4b01399e5f127900d4d9f9039f5972e07577c39e))
* select current_schema ([#273](https://github.com/GoogleCloudPlatform/pgadapter/issues/273)) ([b80069d](https://github.com/GoogleCloudPlatform/pgadapter/commit/b80069d555c593730aa31a36b4c77e037f9c52b1))
* set default PG version reported by PGAdapter to 14.1 ([#303](https://github.com/GoogleCloudPlatform/pgadapter/issues/303)) ([4dcceb1](https://github.com/GoogleCloudPlatform/pgadapter/commit/4dcceb1916fb63ed8b27d247aaa51d5a4404d0dd))
* support (ignore) show and set search_path ([#288](https://github.com/GoogleCloudPlatform/pgadapter/issues/288)) ([93d8c14](https://github.com/GoogleCloudPlatform/pgadapter/commit/93d8c140bfbcdc17daaaece4a66b649d701f7777))
* support pg_settings table as CTE ([#307](https://github.com/GoogleCloudPlatform/pgadapter/issues/307)) ([a5e5634](https://github.com/GoogleCloudPlatform/pgadapter/commit/a5e563419cd0cc83557719991469ff606429a976))


### Bug Fixes

* ( and ) are allowed at the end of keywords ([#312](https://github.com/GoogleCloudPlatform/pgadapter/issues/312)) ([f3ebfb5](https://github.com/GoogleCloudPlatform/pgadapter/commit/f3ebfb58a6f20ade8e3ad295038ee9a906faa65b))
* allow 'T' in timestamp values in COPY operations ([#319](https://github.com/GoogleCloudPlatform/pgadapter/issues/319)) ([a239328](https://github.com/GoogleCloudPlatform/pgadapter/commit/a2393283c01c3b46da874e0b0b457da24794a1d6))
* backslash is not a valid quote escape ([#317](https://github.com/GoogleCloudPlatform/pgadapter/issues/317)) ([dc32af4](https://github.com/GoogleCloudPlatform/pgadapter/commit/dc32af4a7cd6b4546292c2936b805a635acf8bf1))
* catch unknown types in RowDescription ([#343](https://github.com/GoogleCloudPlatform/pgadapter/issues/343)) ([6562014](https://github.com/GoogleCloudPlatform/pgadapter/commit/65620142574a61070b13f397e2fb9caeb44670c1))
* correctly detect end of unquoted identifier ([#301](https://github.com/GoogleCloudPlatform/pgadapter/issues/301)) ([e31fd02](https://github.com/GoogleCloudPlatform/pgadapter/commit/e31fd021ff0a6c27aa5b2b31e71c6cb371d0bde2))
* hint for large copy operations missed 'spanner.' namespace ([#304](https://github.com/GoogleCloudPlatform/pgadapter/issues/304)) ([a5e8afc](https://github.com/GoogleCloudPlatform/pgadapter/commit/a5e8afc797c983908a1524032590991a9eaa4ea9))
* remove Spanner error prefixes ([#306](https://github.com/GoogleCloudPlatform/pgadapter/issues/306)) ([819a653](https://github.com/GoogleCloudPlatform/pgadapter/commit/819a6535536ea74ea68e7f21684271fe1b5f51d7))
* translate queries for all table types in JDBC metadata ([#295](https://github.com/GoogleCloudPlatform/pgadapter/issues/295)) ([4cb43e6](https://github.com/GoogleCloudPlatform/pgadapter/commit/4cb43e643f7648f00869247b55a6c3118edbb6d9))
* use a longer timeout for Commit for COPY ([#308](https://github.com/GoogleCloudPlatform/pgadapter/issues/308)) ([0f4dea7](https://github.com/GoogleCloudPlatform/pgadapter/commit/0f4dea7c85ff0d6611ecaa143d37f3e396bd7bab))
* use main thread for CopyDataReceiver ([#345](https://github.com/GoogleCloudPlatform/pgadapter/issues/345)) ([687c952](https://github.com/GoogleCloudPlatform/pgadapter/commit/687c9521ecb081f65062521e39820b7fedfccdd4))


### Performance Improvements

* treat 'flush and sync' as 'sync' ([#285](https://github.com/GoogleCloudPlatform/pgadapter/issues/285)) ([7fc5d51](https://github.com/GoogleCloudPlatform/pgadapter/commit/7fc5d51805722aadf968cb520f789f4f98ac70b8))


### Dependencies

* bump google-cloud-spanner from 6.27.0 to 6.28.0 ([#324](https://github.com/GoogleCloudPlatform/pgadapter/issues/324)) ([029da7e](https://github.com/GoogleCloudPlatform/pgadapter/commit/029da7e03f5c457130bd837e702248088083bf0e))


### Documentation

* document all command line arguments ([#296](https://github.com/GoogleCloudPlatform/pgadapter/issues/296)) ([e4b32e2](https://github.com/GoogleCloudPlatform/pgadapter/commit/e4b32e2a6ec7e7d196ae6e0f9d77bb58fcafa64b))
* document COPY TO STDOUT ([#297](https://github.com/GoogleCloudPlatform/pgadapter/issues/297)) ([3dbb19f](https://github.com/GoogleCloudPlatform/pgadapter/commit/3dbb19f0293d2e9d79ab84f3c1a2c5d389424b5a))
* document psycopg2 usage ([#344](https://github.com/GoogleCloudPlatform/pgadapter/issues/344)) ([4144c2d](https://github.com/GoogleCloudPlatform/pgadapter/commit/4144c2d61774d74b4982654fa381198ec899a8ac))
* Liquibase samples and tests ([#291](https://github.com/GoogleCloudPlatform/pgadapter/issues/291)) ([d471056](https://github.com/GoogleCloudPlatform/pgadapter/commit/d471056e4e775b6469e740fce465b30d57cdb0f9))

## [0.6.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.6.0...v0.6.1) (2022-07-13)


### Bug Fixes

* prepared statement could return error 'This ResultSet is closed' ([#279](https://github.com/GoogleCloudPlatform/pgadapter/issues/279)) ([3383738](https://github.com/GoogleCloudPlatform/pgadapter/commit/33837384b2c926e268934a082406056070f1f21b)), closes [#278](https://github.com/GoogleCloudPlatform/pgadapter/issues/278)

## [0.6.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.5.1...v0.6.0) (2022-07-11)


### Features

* COPY my_table FROM STDIN BINARY ([#261](https://github.com/GoogleCloudPlatform/pgadapter/issues/261)) ([7155783](https://github.com/GoogleCloudPlatform/pgadapter/commit/7155783f69f1250f1310ab0a4108f9b65e7bc757))
* COPY my_table TO STDOUT ([#269](https://github.com/GoogleCloudPlatform/pgadapter/issues/269)) ([393b520](https://github.com/GoogleCloudPlatform/pgadapter/commit/393b52061df760ebfccd911f56ed0b29e539c9d5))
* COPY my_table TO STDOUT BINARY  ([#271](https://github.com/GoogleCloudPlatform/pgadapter/issues/271)) ([d8c4c77](https://github.com/GoogleCloudPlatform/pgadapter/commit/d8c4c77c1a60efdf2132b63f7b06ebda273b61f0))
* support \l meta command ([#215](https://github.com/GoogleCloudPlatform/pgadapter/issues/215)) ([b9d0363](https://github.com/GoogleCloudPlatform/pgadapter/commit/b9d03630b4b6e1d659b5498cd05acc70b798fbb8))
* support COPY statement in a batch of sql statements ([#217](https://github.com/GoogleCloudPlatform/pgadapter/issues/217)) ([d39cec8](https://github.com/GoogleCloudPlatform/pgadapter/commit/d39cec800d7738b8d0708fd7f3a0f99497854846))
* support IF [NOT] EXISTS for DDL statements ([#224](https://github.com/GoogleCloudPlatform/pgadapter/issues/224)) ([703a25d](https://github.com/GoogleCloudPlatform/pgadapter/commit/703a25dc62312b6143dd7cbf87f54ca86cb51352))


### Bug Fixes

* copy could return wrong error message ([#252](https://github.com/GoogleCloudPlatform/pgadapter/issues/252)) ([6ad4aa2](https://github.com/GoogleCloudPlatform/pgadapter/commit/6ad4aa26b950026fbd9e8d1cf0f93d4d11dbf990))
* COPY null values caused NullPointerException ([#254](https://github.com/GoogleCloudPlatform/pgadapter/issues/254)) ([cd34476](https://github.com/GoogleCloudPlatform/pgadapter/commit/cd3447683e055fe37eefbaf732b3662f8884707c))
* order parameters by index and not textual value ([#239](https://github.com/GoogleCloudPlatform/pgadapter/issues/239)) ([d472639](https://github.com/GoogleCloudPlatform/pgadapter/commit/d472639625e3ae2ee4e6ba71bd75a2dc56becb39))
* parse table names with schema prefix ([#232](https://github.com/GoogleCloudPlatform/pgadapter/issues/232)) ([cbdf28d](https://github.com/GoogleCloudPlatform/pgadapter/commit/cbdf28dcd3ae0299f49b335c2effe398a55e9c87))
* respect result format code from Bind msg ([#238](https://github.com/GoogleCloudPlatform/pgadapter/issues/238)) ([708fa42](https://github.com/GoogleCloudPlatform/pgadapter/commit/708fa42137966a580b0309e751b326da30f107f2))
* skip bytes in an invalid stream defensively ([#241](https://github.com/GoogleCloudPlatform/pgadapter/issues/241)) ([1c60253](https://github.com/GoogleCloudPlatform/pgadapter/commit/1c60253d4879d8fe3f07756a9c05aa319d817e24))


### Documentation

* add authentication faq entry ([#244](https://github.com/GoogleCloudPlatform/pgadapter/issues/244)) ([d5cc7e3](https://github.com/GoogleCloudPlatform/pgadapter/commit/d5cc7e306ef51f67ca411785585bd7592e89f686))
* add connection tips for pgx ([#234](https://github.com/GoogleCloudPlatform/pgadapter/issues/234)) ([1ac2a70](https://github.com/GoogleCloudPlatform/pgadapter/commit/1ac2a706db012f6b3244e2fcb7d9649ddad0ad61))
* add connection tips for pgx ([#234](https://github.com/GoogleCloudPlatform/pgadapter/issues/234)) ([684a068](https://github.com/GoogleCloudPlatform/pgadapter/commit/684a0680bdbf03881c14bcb45dea9240f94f416a))
* document JDBC connection tips and performance considerations ([#233](https://github.com/GoogleCloudPlatform/pgadapter/issues/233)) ([5a09690](https://github.com/GoogleCloudPlatform/pgadapter/commit/5a096901b47acbb86d5087f9564745d871569a90))
* move COPY documentation to separate file ([#246](https://github.com/GoogleCloudPlatform/pgadapter/issues/246)) ([54251aa](https://github.com/GoogleCloudPlatform/pgadapter/commit/54251aaee78f8c7b789be57d5606a8ffac8d5c7d))
* update readme to reflect latest version ([#229](https://github.com/GoogleCloudPlatform/pgadapter/issues/229)) ([ea998bf](https://github.com/GoogleCloudPlatform/pgadapter/commit/ea998bf03f0a14be5127851432655cc23f09b767))

## [0.5.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.5.0...v0.5.1) (2022-06-22)


### Bug Fixes

* correctly skip nested block comments ([#219](https://github.com/GoogleCloudPlatform/pgadapter/issues/219)) ([c9903da](https://github.com/GoogleCloudPlatform/pgadapter/commit/c9903da8d5fda6bf45f4cbfdff9d243be97f68dd))
* unix domain sockets failed for msg size > 8Kb on MacOS ([#188](https://github.com/GoogleCloudPlatform/pgadapter/issues/188)) ([bc778ec](https://github.com/GoogleCloudPlatform/pgadapter/commit/bc778ec252c9f98c4dabb98d61e532623b6a1ce5))


### Dependencies

* bump junixsockets to 2.5.0 ([#186](https://github.com/GoogleCloudPlatform/pgadapter/issues/186)) ([58d09cb](https://github.com/GoogleCloudPlatform/pgadapter/commit/58d09cb4d94b0e7b3b85fc83f63e4c4e27e80291))
* enable dependabot ([#187](https://github.com/GoogleCloudPlatform/pgadapter/issues/187)) ([1bf14c8](https://github.com/GoogleCloudPlatform/pgadapter/commit/1bf14c8c51aaf55f3038734dab0228d1b96d0b71))
* remove custom Maven plugin versions ([#214](https://github.com/GoogleCloudPlatform/pgadapter/issues/214)) ([1392a7a](https://github.com/GoogleCloudPlatform/pgadapter/commit/1392a7af55a40f58ca287bf0ec537994b097f807))


### Documentation

* add documentation for connection options ([#212](https://github.com/GoogleCloudPlatform/pgadapter/issues/212)) ([837fe31](https://github.com/GoogleCloudPlatform/pgadapter/commit/837fe31cc8777095961e4b402ef70ed754342d19))
* mark drivers as having experimental support ([#189](https://github.com/GoogleCloudPlatform/pgadapter/issues/189)) ([5ab7caa](https://github.com/GoogleCloudPlatform/pgadapter/commit/5ab7caaf9fe0e67e78f6101439ee87d1932cce04))
* update README to reflect recent updates ([#190](https://github.com/GoogleCloudPlatform/pgadapter/issues/190)) ([d0c52bf](https://github.com/GoogleCloudPlatform/pgadapter/commit/d0c52bff3d4ab43012bb94da5bfdbc3689343e50))

## [0.5.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.4.2...v0.5.0) (2022-06-13)


### Features

* add support for describe statement ([#125](https://github.com/GoogleCloudPlatform/pgadapter/issues/125)) ([52452d7](https://github.com/GoogleCloudPlatform/pgadapter/commit/52452d72d284009deea69b29f2a77dd885f0e1fe))
* add support for extended query protocol batching ([#168](https://github.com/GoogleCloudPlatform/pgadapter/issues/168)) ([30607f0](https://github.com/GoogleCloudPlatform/pgadapter/commit/30607f017b0001dce74d2874f1031a98d75ef917))
* automatically detect client that is connected ([#169](https://github.com/GoogleCloudPlatform/pgadapter/issues/169)) ([f11f459](https://github.com/GoogleCloudPlatform/pgadapter/commit/f11f4593a0b5e7e32e70795d4c4bb3b929873672))
* enable authentication ([#174](https://github.com/GoogleCloudPlatform/pgadapter/issues/174)) ([2e34c84](https://github.com/GoogleCloudPlatform/pgadapter/commit/2e34c84f954a770902374d9f16ae9c41e1342ee9))
* handle implicit transactions and errors in batches ([#127](https://github.com/GoogleCloudPlatform/pgadapter/issues/127)) ([23702ee](https://github.com/GoogleCloudPlatform/pgadapter/commit/23702ee0e584f767fb5d62162f5961ed10d8e90a))
* support unix domain sockets ([#150](https://github.com/GoogleCloudPlatform/pgadapter/issues/150)) ([cec7d43](https://github.com/GoogleCloudPlatform/pgadapter/commit/cec7d43bc49f6d2140c420449bb45927442ddf0d))


### Bug Fixes

* clear statement tag before auto rollback ([#147](https://github.com/GoogleCloudPlatform/pgadapter/issues/147)) ([994976f](https://github.com/GoogleCloudPlatform/pgadapter/commit/994976ff675661a2877b7a87e8a3b21560404b23)), closes [#146](https://github.com/GoogleCloudPlatform/pgadapter/issues/146)
* error handling for extended query protocol ([#149](https://github.com/GoogleCloudPlatform/pgadapter/issues/149)) ([6696531](https://github.com/GoogleCloudPlatform/pgadapter/commit/669653161974978618e9089dd5ac534b41cd74c5))
* handle errors if domain socket file is invalid ([#164](https://github.com/GoogleCloudPlatform/pgadapter/issues/164)) ([4a6d865](https://github.com/GoogleCloudPlatform/pgadapter/commit/4a6d865441f509dd19470c751e82f2d605dc4389))
* lower case single statement commands were not recognized ([#148](https://github.com/GoogleCloudPlatform/pgadapter/issues/148)) ([f069eaf](https://github.com/GoogleCloudPlatform/pgadapter/commit/f069eaf6f3e4823839b6841cc95f5bbbba946ec5))
* print version number at startup ([#142](https://github.com/GoogleCloudPlatform/pgadapter/issues/142)) ([9f4d230](https://github.com/GoogleCloudPlatform/pgadapter/commit/9f4d23063ca9bef6080c7617bded50d1607abbfd))
* send comments to the backend to support hints ([#170](https://github.com/GoogleCloudPlatform/pgadapter/issues/170)) ([f329578](https://github.com/GoogleCloudPlatform/pgadapter/commit/f3295786ddcb48e0dee6b390009c5cfde6f96ee7))
* split statement did not correctly parse escaped quotes ([#152](https://github.com/GoogleCloudPlatform/pgadapter/issues/152)) ([cfbec96](https://github.com/GoogleCloudPlatform/pgadapter/commit/cfbec96654a522cb6db776828a76c576e5421de9))
* terminate connection for invalid messages ([#154](https://github.com/GoogleCloudPlatform/pgadapter/issues/154)) ([6ed6266](https://github.com/GoogleCloudPlatform/pgadapter/commit/6ed6266ff7c625ccdd2824db2bf73fb76a512b83))
* use assembly instead of fat jar ([#145](https://github.com/GoogleCloudPlatform/pgadapter/issues/145)) ([dd8d3d1](https://github.com/GoogleCloudPlatform/pgadapter/commit/dd8d3d13ef2c388ca50dc572e9966af8d6c01f6b))


### Dependencies

* bump Spanner to 6.24 ([#155](https://github.com/GoogleCloudPlatform/pgadapter/issues/155)) ([836271e](https://github.com/GoogleCloudPlatform/pgadapter/commit/836271e3147e0a89734668bc5117640e7e3adc2a))
* bump Spanner to 6.25 ([#161](https://github.com/GoogleCloudPlatform/pgadapter/issues/161)) ([d66e0e6](https://github.com/GoogleCloudPlatform/pgadapter/commit/d66e0e612537e0245866c05dd5842ccef944edfa))
* bump Spanner to 6.25.5 ([#172](https://github.com/GoogleCloudPlatform/pgadapter/issues/172)) ([c986a25](https://github.com/GoogleCloudPlatform/pgadapter/commit/c986a25a184d1a3c9d617989bd1c94f966eecb00))


### Documentation

* add FAQ and DDL options documentation ([#171](https://github.com/GoogleCloudPlatform/pgadapter/issues/171)) ([e3016d1](https://github.com/GoogleCloudPlatform/pgadapter/commit/e3016d144e1a538e7bebae2666d7a2ad860af8b6))

### [0.4.2](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.4.1...v0.4.2) (2022-05-03)


### Dependencies

* bump PostgreSQL JDBC to 42.3.4 ([#138](https://github.com/GoogleCloudPlatform/pgadapter/issues/138)) ([9b67670](https://github.com/GoogleCloudPlatform/pgadapter/commit/9b676707f002dfed89f573039a1e83a8517cded0))
* bump Spanner to 6.23.3 ([#137](https://github.com/GoogleCloudPlatform/pgadapter/issues/137)) ([180b25c](https://github.com/GoogleCloudPlatform/pgadapter/commit/180b25c65b00c0ea10b92b121c46178c9c1d8006))

### [0.4.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.4.0...v0.4.1) (2022-05-02)


### Bug Fixes

* update documentation to reflect \c support ([#132](https://github.com/GoogleCloudPlatform/pgadapter/issues/132)) ([10798df](https://github.com/GoogleCloudPlatform/pgadapter/commit/10798dfe51cc61bba7bdc3919cdabb5edd4dce6e))

## [0.4.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.3.0...v0.4.0) (2022-04-29)


### Features

* all statement batching ([#108](https://github.com/GoogleCloudPlatform/pgadapter/issues/108)) ([1d88311](https://github.com/GoogleCloudPlatform/pgadapter/commit/1d88311b0622385499c718a24b8d2f295f6aa8bc))
* support connecting to different databases ([#121](https://github.com/GoogleCloudPlatform/pgadapter/issues/121)) ([2bc0355](https://github.com/GoogleCloudPlatform/pgadapter/commit/2bc03550a3b00f8f188733b1c1cdf101b813dbdc))
* support fetching rows in multiple steps ([#122](https://github.com/GoogleCloudPlatform/pgadapter/issues/122)) ([d5b76ca](https://github.com/GoogleCloudPlatform/pgadapter/commit/d5b76caab0f74330f9905560e35e4ff9f6197a3f))
* support more JDBC parameter types ([#118](https://github.com/GoogleCloudPlatform/pgadapter/issues/118)) ([21a7bd0](https://github.com/GoogleCloudPlatform/pgadapter/commit/21a7bd0a3e2d7df77270a83cec3a8bca5c321a15))
* support pgx in extended mode ([#82](https://github.com/GoogleCloudPlatform/pgadapter/issues/82)) ([1fbb35d](https://github.com/GoogleCloudPlatform/pgadapter/commit/1fbb35d929f9b6786852424b32b43da22dbf2262))


### Bug Fixes

* allow QueryMessage with empty query string ([#113](https://github.com/GoogleCloudPlatform/pgadapter/issues/113)) ([2a3f2eb](https://github.com/GoogleCloudPlatform/pgadapter/commit/2a3f2eb7cd16cc3c71ffc673544ca9e2baaba21c))
* invalid metadata command ([#124](https://github.com/GoogleCloudPlatform/pgadapter/issues/124)) ([4d2d37d](https://github.com/GoogleCloudPlatform/pgadapter/commit/4d2d37d34271f7404a468707ee18aa7971af092c))
* remember parameter types of parsed statement ([#114](https://github.com/GoogleCloudPlatform/pgadapter/issues/114)) ([8f0d477](https://github.com/GoogleCloudPlatform/pgadapter/commit/8f0d47785e04dc9db6ee275074777849f006d797))
* return EmptyQueryResponse for empty statements ([#126](https://github.com/GoogleCloudPlatform/pgadapter/issues/126)) ([77e7421](https://github.com/GoogleCloudPlatform/pgadapter/commit/77e7421958a7b8e06a231729aa3e3cf50fbf78b7))


### Documentation

* add Maven coordinates to README ([#111](https://github.com/GoogleCloudPlatform/pgadapter/issues/111)) ([3629b54](https://github.com/GoogleCloudPlatform/pgadapter/commit/3629b541ecb232b772d2f70c33d66b6055636300))
* udpate pom description ([#112](https://github.com/GoogleCloudPlatform/pgadapter/issues/112)) ([f7d05b2](https://github.com/GoogleCloudPlatform/pgadapter/commit/f7d05b2c355bbd4431b2babfa7db732db64c5b20))

## [0.3.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.2.1...v0.3.0) (2022-04-07)


### Features

* add support for DATE data type ([#88](https://github.com/GoogleCloudPlatform/pgadapter/issues/88)) ([56e2015](https://github.com/GoogleCloudPlatform/pgadapter/commit/56e2015a490b342c97c79b46b1443feb21021258))


### Performance Improvements

* reduce parsing of sql string ([#79](https://github.com/GoogleCloudPlatform/pgadapter/issues/79)) ([4c24ef9](https://github.com/GoogleCloudPlatform/pgadapter/commit/4c24ef9d06d0601d426ef97d51bd207c91fd148e))
* skip analyzeQuery for queries ([#80](https://github.com/GoogleCloudPlatform/pgadapter/issues/80)) ([98e430a](https://github.com/GoogleCloudPlatform/pgadapter/commit/98e430aa97ebfa7fe9987cad4c8316c57af09928))


### Documentation

* update README with instructions for running ([#97](https://github.com/GoogleCloudPlatform/pgadapter/issues/97)) ([bab6106](https://github.com/GoogleCloudPlatform/pgadapter/commit/bab6106aeface8bb19ef72896ded8d4131f79d7e))

### [0.2.1](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.2.0...v0.2.1) (2022-03-31)


### Bug Fixes

* handle null values in arrays ([#87](https://github.com/GoogleCloudPlatform/pgadapter/issues/87)) ([b469d7f](https://github.com/GoogleCloudPlatform/pgadapter/commit/b469d7f981e81d82d288c76ed11bf3db345bc3e4))


### Performance Improvements

* reduce logger overhead ([#78](https://github.com/GoogleCloudPlatform/pgadapter/issues/78)) ([8ba1004](https://github.com/GoogleCloudPlatform/pgadapter/commit/8ba1004a1e03c3fa190ed0ba76e53704b9fd8137))

## [0.2.0](https://github.com/GoogleCloudPlatform/pgadapter/compare/v0.1.0...v0.2.0) (2022-03-30)


### Features

* User agent header addition ([#46](https://github.com/GoogleCloudPlatform/pgadapter/issues/46)) ([9e891de](https://github.com/GoogleCloudPlatform/pgadapter/commit/9e891de3660e5b8059136b9ff4f7b40c5fe7ba81))


### Bug Fixes

* docker build command in GitHub Actions config ([#90](https://github.com/GoogleCloudPlatform/pgadapter/issues/90)) ([ec210ca](https://github.com/GoogleCloudPlatform/pgadapter/commit/ec210ca3ff8dd7e6882e5fb3e8c3988614900c53))
* handle connection failures better by returning an error response ([#67](https://github.com/GoogleCloudPlatform/pgadapter/issues/67)) ([c0bad6f](https://github.com/GoogleCloudPlatform/pgadapter/commit/c0bad6faf4a2870107801030d2f9188cfe2f4ba9))


### Performance Improvements

* reduce flushing the output stream ([#77](https://github.com/GoogleCloudPlatform/pgadapter/issues/77)) ([4871bc5](https://github.com/GoogleCloudPlatform/pgadapter/commit/4871bc52b7155734d716de2862ae154053f620e6))

## 0.1.0 (2022-03-18)


### Features

* Add Copy command parsing in QueryMessage and basic psql e2e test ([#43](https://github.com/GoogleCloudPlatform/pgadapter/issues/43)) ([184c00e](https://github.com/GoogleCloudPlatform/pgadapter/commit/184c00e6191521cb10bd518f97e5022e9c9e442a))
* Add support for multiple CopyData messages ([#40](https://github.com/GoogleCloudPlatform/pgadapter/issues/40)) ([24eeedc](https://github.com/GoogleCloudPlatform/pgadapter/commit/24eeedcbc979e7aff5c9db895fcd8f2f49f62eaf))
* add support incoming binary values ([#27](https://github.com/GoogleCloudPlatform/pgadapter/issues/27)) ([2ef7563](https://github.com/GoogleCloudPlatform/pgadapter/commit/2ef7563a059cc444d03031b7d9326755c3900fc4))
* COPY supports large files and streaming input from PostgreSQL ([#52](https://github.com/GoogleCloudPlatform/pgadapter/issues/52)) ([b773999](https://github.com/GoogleCloudPlatform/pgadapter/commit/b773999b0a89d6a13247348004803f062b201555))
* enable native JDBC connections ([#28](https://github.com/GoogleCloudPlatform/pgadapter/issues/28)) ([ceba433](https://github.com/GoogleCloudPlatform/pgadapter/commit/ceba43392b81a08602ebbca43b9bebf570c119ec))
* support JDBC metadata queries ([#58](https://github.com/GoogleCloudPlatform/pgadapter/issues/58)) ([021e131](https://github.com/GoogleCloudPlatform/pgadapter/commit/021e13124805df713f8e66e4e875721754b8e890))
* trigger release ([#63](https://github.com/GoogleCloudPlatform/pgadapter/issues/63)) ([62af37d](https://github.com/GoogleCloudPlatform/pgadapter/commit/62af37d727c1ea4238235a2a45cb5cb42107a12c))


### Bug Fixes

* add newly added view to expected result ([4ca4411](https://github.com/GoogleCloudPlatform/pgadapter/commit/4ca441186d934c710013a7ef25d26bb9c6bf84e4))
* add support for arrays in ResultSets ([#36](https://github.com/GoogleCloudPlatform/pgadapter/issues/36)) ([90bd661](https://github.com/GoogleCloudPlatform/pgadapter/commit/90bd66103ec0874ce3c2c3c81b910b023e9ebf09))
* CI integration tests ignored environment variable values ([#31](https://github.com/GoogleCloudPlatform/pgadapter/issues/31)) ([c37d2e4](https://github.com/GoogleCloudPlatform/pgadapter/commit/c37d2e4b38cfb14d24721b4ced3c96c783ae3d21))
* command should determine result type ([#29](https://github.com/GoogleCloudPlatform/pgadapter/issues/29)) ([1a39338](https://github.com/GoogleCloudPlatform/pgadapter/commit/1a39338e6febaf09f998cececbe5b979049c64e2))
* remove all GSQL headers ([#60](https://github.com/GoogleCloudPlatform/pgadapter/issues/60)) ([755592a](https://github.com/GoogleCloudPlatform/pgadapter/commit/755592a9ff16afded6c16e8bd49e4fdddcf1be04))
* return correct transaction status ([69c4017](https://github.com/GoogleCloudPlatform/pgadapter/commit/69c4017fb490101e5d14d2a8c0abe40d38c0e9a6))
* Statements with no results would return an error ([#57](https://github.com/GoogleCloudPlatform/pgadapter/issues/57)) ([398afbe](https://github.com/GoogleCloudPlatform/pgadapter/commit/398afbe237e47df9c3e28042c4031081214f0d07)), closes [#56](https://github.com/GoogleCloudPlatform/pgadapter/issues/56)
* support null parameters ([#35](https://github.com/GoogleCloudPlatform/pgadapter/issues/35)) ([4fde6c3](https://github.com/GoogleCloudPlatform/pgadapter/commit/4fde6c3261178802c56d2f575e9f2e15fe5b0721))
* Update Copy parser to handle ',' separated option list ([#49](https://github.com/GoogleCloudPlatform/pgadapter/issues/49)) ([7c6530f](https://github.com/GoogleCloudPlatform/pgadapter/commit/7c6530fd12d568aca2a1ff48b66adecd67955e2f))


### Dependencies

* bump Spanner client lib to 6.21 ([#54](https://github.com/GoogleCloudPlatform/pgadapter/issues/54)) ([020471b](https://github.com/GoogleCloudPlatform/pgadapter/commit/020471bc676d4ed7672c82edbc06bb867fd38cbb))
* upgrade jdbc to 2.5.6-pg-SNAPSHOT ([f3f0f87](https://github.com/GoogleCloudPlatform/pgadapter/commit/f3f0f87b1a4483f5246e857cc9ffdcdd871dd37b))
* upgrade jdbc to 2.5.7-pg-SNAPSHOT ([0f61776](https://github.com/GoogleCloudPlatform/pgadapter/commit/0f61776ac4f8eba5c7d6b2ab0178851fe17e58e5))
