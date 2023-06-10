// Copyright 2023 Google LLC
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

import {randomUUID} from "crypto";

const firstNames: string[] = [
  "Elijah",
  "Sophia",
  "Jackson",
  "Olivia",
  "Mason",
  "Ava",
  "Ethan",
  "Emma",
  "Aiden",
  "Isabella",
  "Lucas",
  "Mia"
];
const lastNames: string[] = [
  "Smith",
  "Johnson",
  "Williams",
  "Jones",
  "Brown",
  "Davis",
  "Miller",
  "Wilson",
  "Moore",
  "Taylor",
  "Anderson",
  "Thomas"
];
const nouns: string[] = [
  "elephant",
  "fireplace",
  "jazz",
  "lighthouse",
  "moonlight",
  "paradise",
  "quill",
  "seashell",
  "thunder",
  "volcano",
  "waterfall",
  "zebra"
];
const adjectives: string[] = [
  "brilliant",
  "enchanting",
  "magnificent",
  "serene",
  "vibrant",
  "charismatic",
  "exquisite",
  "radiant",
  "whimsical",
  "majestic",
  "captivating",
  "phenomenal"
];
const verbs: string[] = [
  "drive",
  "fly",
  "cook",
  "clean",
  "run",
  "swim",
  "cycle",
  "sing",
  "dance",
  "write",
  "read",
  "sleep",
  "eat",
  "drink",
  "talk",
  "listen",
  "play",
  "study",
  "work",
  "code",
  "paint",
  "draw",
  "climb",
  "explore",
  "travel"
];
const adverbs: string[] = [
  "naturally",
  "gracefully",
  "vigorously",
  "curiously",
  "cheerfully",
  "cautiously",
  "enthusiastically",
  "generously",
  "politely",
  "diligently",
  "frequently",
  "precisely",
  "efficiently",
  "passionately",
  "quickly",
  "slowly",
  "happily",
  "sadly",
  "loudly",
  "softly",
  "carefully",
  "eagerly",
  "gently",
  "quietly",
  "kindly",
  "boldly",
  "patiently",
  "bravely"
];


export const times = (n, f) => { while(n-- > 0) f(); }

export function randomId(): string {
  return randomUUID();
}

export function randomFirstName(): string {
  return randomElement(firstNames);
}

export function randomLastName(): string {
  return randomElement(lastNames);
}

export function randomAlbumTitle(): string {
  return randomElement(adjectives) + " " + randomElement(nouns);
}

export function randomTrackTitle(): string {
  return randomElement(adverbs) + " " + randomElement(verbs);
}

export function randomReleaseDate(): Date {
  return randomDate(new Date("1850-01-01"), new Date("2023-04-18"));
}

export function randomDate(from: Date, to: Date): Date {
  const fromTime = from.getTime();
  const toTime = to.getTime();
  const random = new Date(fromTime + Math.random() * (toTime - fromTime));
  return new Date(random.setUTCHours(0, 0, 0, 0));
}

export function randomElement(arr: Array<any>) {
  return arr[Math.floor(Math.random() * arr.length)]
}