"use strict";
// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const init_1 = require("./init");
const models_1 = require("../models/models");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        yield (0, init_1.startSequelize)();
        (0, models_1.initModels)();
        // Create a new singer record.
        const singer = yield models_1.Singer.create({ firstName: "Jane", lastName: "Doe" });
        console.log(`Created a new singer record with ID ${singer.id} at ${singer.createdAt}`);
        const album = yield models_1.Album.create({ title: "My first Album", SingerId: singer.id });
        console.log(`Created a new album record with ID ${album.id}`);
        yield (0, init_1.shutdownSequelize)();
    });
}
(() => __awaiter(void 0, void 0, void 0, function* () {
    yield main();
}))().catch(e => {
    console.error(e);
});
