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
Object.defineProperty(exports, "__esModule", { value: true });
exports.adverbs = exports.verbs = exports.nouns = exports.adjectives = exports.randomTrackTitle = exports.randomAlbumTitle = exports.randomLastName = exports.randomFirstName = exports.randomInt = void 0;
function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1) + min);
}
exports.randomInt = randomInt;
function randomFirstName() {
    return randomArrayElement(first_names);
}
exports.randomFirstName = randomFirstName;
function randomLastName() {
    return randomArrayElement(last_names);
}
exports.randomLastName = randomLastName;
function randomAlbumTitle() {
    return `${randomArrayElement(exports.adjectives)} ${randomArrayElement(exports.nouns)}`;
}
exports.randomAlbumTitle = randomAlbumTitle;
function randomTrackTitle() {
    return `${randomArrayElement(exports.adverbs)} ${randomArrayElement(exports.verbs)}`;
}
exports.randomTrackTitle = randomTrackTitle;
function randomArrayElement(array) {
    return array[Math.floor(Math.random() * array.length)];
}
const first_names = [
    "Saffron", "Eleanor", "Ann", "Salma", "Kiera", "Mariam", "Georgie", "Eden", "Carmen", "Darcie",
    "Antony", "Benjamin", "Donald", "Keaton", "Jared", "Simon", "Tanya", "Julian", "Eugene", "Laurence"
];
const last_names = [
    "Terry", "Ford", "Mills", "Connolly", "Newton", "Rodgers", "Austin", "Floyd", "Doherty", "Nguyen",
    "Chavez", "Crossley", "Silva", "George", "Baldwin", "Burns", "Russell", "Ramirez", "Hunter", "Fuller"
];
exports.adjectives = [
    "ultra",
    "happy",
    "emotional",
    "filthy",
    "charming",
    "alleged",
    "talented",
    "exotic",
    "lamentable",
    "lewd",
    "old-fashioned",
    "savory",
    "delicate",
    "willing",
    "habitual",
    "upset",
    "gainful",
    "nonchalant",
    "kind",
    "unruly"
];
exports.nouns = [
    "improvement",
    "control",
    "tennis",
    "gene",
    "department",
    "person",
    "awareness",
    "health",
    "development",
    "platform",
    "garbage",
    "suggestion",
    "agreement",
    "knowledge",
    "introduction",
    "recommendation",
    "driver",
    "elevator",
    "industry",
    "extent"
];
exports.verbs = [
    "instruct",
    "rescue",
    "disappear",
    "import",
    "inhibit",
    "accommodate",
    "dress",
    "describe",
    "mind",
    "strip",
    "crawl",
    "lower",
    "influence",
    "alter",
    "prove",
    "race",
    "label",
    "exhaust",
    "reach",
    "remove"
];
exports.adverbs = [
    "cautiously",
    "offensively",
    "immediately",
    "soon",
    "judgementally",
    "actually",
    "honestly",
    "slightly",
    "limply",
    "rigidly",
    "fast",
    "normally",
    "unnecessarily",
    "wildly",
    "unimpressively",
    "helplessly",
    "rightfully",
    "kiddingly",
    "early",
    "queasily"
];
