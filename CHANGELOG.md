# Changelog

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
