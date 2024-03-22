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


export function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

export function randomFirstName(): string {
  return randomArrayElement(first_names);
}

export function randomLastName(): string {
  return randomArrayElement(last_names);
}

export function randomAlbumTitle(): string {
  return `${randomArrayElement(adjectives)} ${randomArrayElement(nouns)}`;
}

export function randomTrackTitle(): string {
  return `${randomArrayElement(adverbs)} ${randomArrayElement(verbs)}`;
}

function randomArrayElement(array: Array<string>): string {
  return array[Math.floor(Math.random() * array.length)];
}

const first_names: string[] = [
  "Saffron", "Eleanor", "Ann", "Salma", "Kiera", "Mariam", "Georgie", "Eden", "Carmen", "Darcie",
  "Antony", "Benjamin", "Donald", "Keaton", "Jared", "Simon", "Tanya", "Julian", "Eugene", "Laurence"];

const last_names: string[] = [
  "Terry", "Ford", "Mills", "Connolly", "Newton", "Rodgers", "Austin", "Floyd", "Doherty", "Nguyen",
  "Chavez", "Crossley", "Silva", "George", "Baldwin", "Burns", "Russell", "Ramirez", "Hunter", "Fuller"];

export const adjectives: string[] = [
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
  "unruly"];

export const nouns: string[] = [
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
  "extent"];

export const verbs: string[] = [
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
  "remove"];

export const adverbs: string[] = [
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
  "queasily"];
