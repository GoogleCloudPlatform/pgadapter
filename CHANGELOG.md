# Changelog

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
