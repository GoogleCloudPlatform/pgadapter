# Changelog

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
