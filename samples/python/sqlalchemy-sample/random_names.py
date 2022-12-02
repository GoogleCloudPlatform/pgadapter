import secrets
from random import seed, randrange, random
from datetime import datetime, date


seed()

def random_first_name():
  return first_names[randrange(len(first_names))]


def random_last_name():
  return last_names[randrange(len(last_names))]


def random_album_title():
  return "{} {}".format(
    adjectives[randrange(len(adjectives))], nouns[randrange(len(nouns))])


def random_release_date():
  return date.fromisoformat(
    "{}-{:02d}-{:02d}".format(randrange(1900, 2023),
                              randrange(1, 13),
                              randrange(1, 29)))


def random_marketing_budget():
  return random() * 1000000


def random_cover_picture():
  return secrets.token_bytes(randrange(1, 10))


first_names = [
  "Saffron", "Eleanor", "Ann", "Salma", "Kiera", "Mariam", "Georgie", "Eden", "Carmen", "Darcie",
  "Antony", "Benjamin", "Donald", "Keaton", "Jared", "Simon", "Tanya", "Julian", "Eugene", "Laurence"
]
last_names = [
  "Terry", "Ford", "Mills", "Connolly", "Newton", "Rodgers", "Austin", "Floyd", "Doherty", "Nguyen",
  "Chavez", "Crossley", "Silva", "George", "Baldwin", "Burns", "Russell", "Ramirez", "Hunter", "Fuller"
]
adjectives = [
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
]
nouns = [
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
]
verbs = [
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
]
adverbs = [
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
]
