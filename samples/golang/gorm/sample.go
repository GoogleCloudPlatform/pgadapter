// Copyright 2022 Google LLC
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

package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// TODO(developer): Change this to match your PGAdapter instance and database name
var connectionString = "host=/tmp port=5432 database=gorm-sample"

// BaseModel is embedded in all other models to add common database fields.
type BaseModel struct {
	// ID is the primary key of each model. The ID is generated client side as a UUID.
	// Adding the `primaryKey` annotation is redundant for most models, as gorm will assume that the column with name ID
	// is the primary key. This is however not redundant for models that add additional primary key columns, such as
	// child tables in interleaved table hierarchies, as a missing primary key annotation here would then cause the
	// primary key column defined on the child table to be the only primary key column.
	ID string `gorm:"primaryKey;autoIncrement:false"`
	// CreatedAt and UpdatedAt are managed automatically by gorm.
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Singer struct {
	BaseModel
	FirstName sql.NullString
	LastName  string
	// FullName is generated by the database. The '->' marks this a read-only field. Preferably this field should also
	// include a `default:(-)` annotation, as that would make gorm read the value back using a RETURNING clause. That is
	// however currently not supported.
	FullName string `gorm:"->;type:GENERATED ALWAYS AS (coalesce(concat(first_name,' '::varchar,last_name))) STORED;"`
	Active   bool
	Albums   []Album
}

type Album struct {
	BaseModel
	Title           string
	MarketingBudget decimal.NullDecimal
	ReleaseDate     datatypes.Date
	CoverPicture    []byte
	SingerId        string
	Singer          Singer
	Tracks          []Track `gorm:"foreignKey:ID"`
}

// Track is interleaved in Album. The ID column is both the first part of the primary key of Track, and a
// reference to the Album that owns the Track.
type Track struct {
	BaseModel
	TrackNumber int64 `gorm:"primaryKey;autoIncrement:false"`
	Title       string
	SampleRate  float64
	Album       Album `gorm:"foreignKey:ID"`
}

type Venue struct {
	BaseModel
	Name        string
	Description string
}

type Concert struct {
	BaseModel
	Name      string
	Venue     Venue
	VenueId   string
	Singer    Singer
	SingerId  string
	StartTime time.Time
	EndTime   time.Time
}

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		fmt.Printf("Failed to open gorm connection: %v\n", err)
		return
	}
	conn, err := db.DB()
	if err != nil {
		fmt.Printf("Failed to open database connection: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Starting sample...")

	// Delete all existing data to start with a clean database.
	DeleteAllData(db)
	fmt.Print("Purged all existing test data\n\n")

	// Create some random Singers, Albums and Tracks.
	fmt.Println("Creating random singers and albums")
	CreateRandomSingersAndAlbums(db)
	// Print the generated Singers, Albums and Tracks.
	PrintSingersAlbumsAndTracks(db)

	// Create a Concert for a random singer.
	CreateVenueAndConcertInTransaction(db)
	// Print all Concerts in the database.
	PrintConcerts(db)
	// Print all Albums that were released before 1900.
	PrintAlbumsReleaseBefore1900(db)
	// Print all Singers ordered by last name.
	// The function executes multiple queries to fetch a batch of singers per query.
	PrintSingersWithLimitAndOffset(db)
	// Print all Albums that have a title where the first character of the title matches
	// either the first character of the first name or first character of the last name
	// of the Singer.
	PrintAlbumsFirstCharTitleAndFirstOrLastNameEqual(db)
	// Print all Albums whose title start with 'e'. The function uses a named argument for the query.
	SearchAlbumsUsingNamedArgument(db, "e%")

	// Update Venue description.
	UpdateVenueDescription(db)
	// Use FirstOrInit to create or update a Venue.
	FirstOrInitVenue(db, "Berlin Arena")
	// Use FirstOrCreate to create a Venue if it does not already exist.
	FirstOrCreateVenue(db, "Paris Central")
	// Update all Tracks by fetching them in batches and then applying an update to each record.
	UpdateTracksInBatches(db)

	// Delete a random Track from the database.
	DeleteRandomTrack(db)
	// Delete a random Album from the database. This will also delete any child Track records interleaved with the
	// Album.
	DeleteRandomAlbum(db)

	fmt.Printf("Finished running sample\n")
}

// CreateRandomSingersAndAlbums creates some random test records and stores these in the database.
func CreateRandomSingersAndAlbums(db *gorm.DB) {
	// Create between 5 and 10 random singers.
	for i := 0; i < randInt(5, 10); i++ {
		singerId, err := CreateSinger(db, randFirstName(), randLastName())
		if err != nil {
			fmt.Printf("Failed to create singer: %v\n", err)
			return
		}
		fmt.Print(".")
		// Create between 2 and 12 random albums
		for j := 0; j < randInt(2, 12); j++ {
			_, err = CreateAlbumWithRandomTracks(db, singerId, randAlbumTitle(), randInt(1, 22))
			if err != nil {
				fmt.Printf("Failed to create album: %v\n", err)
				return
			}
			fmt.Print(".")
		}
	}
	fmt.Print("\n\n")
}

// PrintSingersAlbumsAndTracks queries and prints all Singers, Albums and Tracks in the database.
func PrintSingersAlbumsAndTracks(db *gorm.DB) {
	fmt.Println("Fetching all singers, albums and tracks")
	var singers []*Singer
	// Preload all associations of Singer.
	if err := db.Model(&Singer{}).Preload(clause.Associations).Order("last_name").Find(&singers).Error; err != nil {
		fmt.Printf("Failed to load all singers: %v\n", err)
		return
	}
	for _, singer := range singers {
		fmt.Printf("Singer: {%v %v}\n", singer.ID, singer.FullName)
		fmt.Printf("Albums:\n")
		for _, album := range singer.Albums {
			fmt.Printf("\tAlbum: {%v %v}\n", album.ID, album.Title)
			fmt.Printf("\tTracks:\n")
			db.Model(&album).Preload(clause.Associations).Find(&album)
			for _, track := range album.Tracks {
				fmt.Printf("\t\tTrack: {%v %v}\n", track.TrackNumber, track.Title)
			}
		}
	}
	fmt.Println()
}

// CreateVenueAndConcertInTransaction creates a new Venue and a Concert in a read/write transaction.
func CreateVenueAndConcertInTransaction(db *gorm.DB) {
	if err := db.Transaction(func(tx *gorm.DB) error {
		// Load the first singer from the database.
		singer := Singer{}
		if res := tx.First(&singer); res.Error != nil {
			return res.Error
		}
		// Create and save a Venue and a Concert for this singer.
		venue := Venue{
			BaseModel:   BaseModel{ID: uuid.NewString()},
			Name:        "Avenue Park",
			Description: `{"Capacity": 5000, "Location": "New York", "Country": "US"}`,
		}
		if res := tx.Create(&venue); res.Error != nil {
			return res.Error
		}
		concert := Concert{
			BaseModel: BaseModel{ID: uuid.NewString()},
			Name:      "Avenue Park Open",
			VenueId:   venue.ID,
			SingerId:  singer.ID,
			StartTime: parseTimestamp("2023-02-01T20:00:00-05:00"),
			EndTime:   parseTimestamp("2023-02-02T02:00:00-05:00"),
		}
		if res := tx.Create(&concert); res.Error != nil {
			return res.Error
		}
		// Return nil to instruct `gorm` to commit the transaction.
		return nil
	}); err != nil {
		fmt.Printf("Failed to create a Venue and a Concert: %v\n", err)
		return
	}
	fmt.Println("Created a concert")
}

// PrintConcerts prints the current concerts in the database to the console.
// It will preload all its associations, so it can directly print the properties of these as well.
func PrintConcerts(db *gorm.DB) {
	var concerts []*Concert
	db.Model(&Concert{}).Preload(clause.Associations).Find(&concerts)
	for _, concert := range concerts {
		fmt.Printf("Concert %q starting at %v will be performed by %s at %s\n",
			concert.Name, concert.StartTime, concert.Singer.FullName, concert.Venue.Name)
	}
	fmt.Println()
}

// UpdateVenueDescription updates the description of the 'Avenue Park' Venue.
func UpdateVenueDescription(db *gorm.DB) {
	if err := db.Transaction(func(tx *gorm.DB) error {
		venue := Venue{}
		if res := tx.Find(&venue, "name = ?", "Avenue Park"); res != nil {
			return res.Error
		}
		// Update the description of the Venue.
		venue.Description = `{"Capacity": 10000, "Location": "New York", "Country": "US", "Type": "Park"}`

		if res := tx.Update("description", &venue); res.Error != nil {
			return res.Error
		}
		// Return nil to instruct `gorm` to commit the transaction.
		return nil
	}); err != nil {
		fmt.Printf("Failed to update Venue 'Avenue Park': %v\n", err)
		return
	}
	fmt.Print("Updated Venue 'Avenue Park'\n\n")
}

// FirstOrInitVenue tries to fetch an existing Venue from the database based on the name of the venue, and if not found,
// initializes a Venue struct. This can then be used to create or update the record.
func FirstOrInitVenue(db *gorm.DB, name string) {
	venue := Venue{}
	if err := db.Transaction(func(tx *gorm.DB) error {
		// Use FirstOrInit to search and otherwise initialize a Venue entity.
		// Note that we do not assign an ID in case the Venue was not found.
		// This makes it possible for us to determine whether we need to call Create or Save, as Cloud Spanner does not
		// support `ON CONFLICT UPDATE` clauses.
		if err := tx.FirstOrInit(&venue, Venue{Name: name}).Error; err != nil {
			return err
		}
		venue.Description = `{"Capacity": 2000, "Location": "Europe/Berlin", "Country": "DE", "Type": "Arena"}`
		// Create or update the Venue.
		if venue.ID == "" {
			return tx.Create(&venue).Error
		}
		return tx.Update("description", &venue).Error
	}); err != nil {
		fmt.Printf("Failed to create or update Venue %q: %v\n", name, err)
		return
	}
	fmt.Printf("Created or updated Venue %q\n\n", name)
}

// FirstOrCreateVenue tries to fetch an existing Venue from the database based on the name of the venue, and if not
// found, creates a new Venue record in the database.
func FirstOrCreateVenue(db *gorm.DB, name string) {
	venue := Venue{}
	if err := db.Transaction(func(tx *gorm.DB) error {
		// Use FirstOrCreate to search and otherwise create a Venue record.
		// Note that we manually assign the ID using the Attrs function. This ensures that the ID is only assigned if
		// the record is not found.
		return tx.Where(Venue{Name: name}).Attrs(Venue{
			BaseModel:   BaseModel{ID: uuid.NewString()},
			Description: `{"Capacity": 5000, "Location": "Europe/Paris", "Country": "FR", "Type": "Stadium"}`,
		}).FirstOrCreate(&venue).Error
	}); err != nil {
		fmt.Printf("Failed to create Venue %q if it did not exist: %v\n", name, err)
		return
	}
	fmt.Printf("Created Venue %q if it did not exist\n\n", name)
}

// UpdateTracksInBatches uses FindInBatches to iterate through a selection of Tracks in batches and updates each Track
// that it found.
func UpdateTracksInBatches(db *gorm.DB) {
	fmt.Print("Updating tracks")
	updated := 0
	if err := db.Transaction(func(tx *gorm.DB) error {
		var tracks []*Track
		return tx.Where("sample_rate > 44.1").FindInBatches(&tracks, 20, func(batchTx *gorm.DB, batch int) error {
			for _, track := range tracks {
				if track.SampleRate > 50 {
					track.SampleRate = track.SampleRate * 0.9
				} else {
					track.SampleRate = track.SampleRate * 0.95
				}
				if res := tx.Model(&track).Update("sample_rate", track.SampleRate); res.Error != nil || res.RowsAffected != int64(1) {
					if res.Error != nil {
						return res.Error
					}
					return fmt.Errorf("update of Track{%s,%d} affected %d rows", track.ID, track.TrackNumber, res.RowsAffected)
				}
				updated++
				fmt.Print(".")
			}
			return nil
		}).Error
	}); err != nil {
		fmt.Printf("\nFailed to batch fetch and update tracks: %v\n", err)
		return
	}
	fmt.Printf("\nUpdated %d tracks\n\n", updated)
}

func PrintAlbumsReleaseBefore1900(db *gorm.DB) {
	fmt.Println("Searching for albums released before 1900")
	var albums []*Album
	db.Where(
		"release_date < ?",
		datatypes.Date(time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)),
	).Order("release_date asc").Find(&albums)
	if len(albums) == 0 {
		fmt.Println("No albums found")
	} else {
		for _, album := range albums {
			fmt.Printf("Album %q was released at %v\n", album.Title, time.Time(album.ReleaseDate).Format("2006-01-02"))
		}
	}
	fmt.Print("\n\n")
}

func PrintSingersWithLimitAndOffset(db *gorm.DB) {
	fmt.Println("Printing all singers ordered by last name")
	var singers []*Singer
	limit := 5
	offset := 0
	for true {
		if err := db.Order("last_name, id").Limit(limit).Offset(offset).Find(&singers).Error; err != nil {
			fmt.Printf("Failed to load singers at offset %d: %v", offset, err)
			return
		}
		if len(singers) == 0 {
			break
		}
		for _, singer := range singers {
			fmt.Printf("%d: %v\n", offset, singer.FullName)
			offset++
		}
	}
	fmt.Printf("Found %d singers\n\n", offset)
}

func PrintAlbumsFirstCharTitleAndFirstOrLastNameEqual(db *gorm.DB) {
	fmt.Println("Searching for albums that have a title that starts with the same character as the first or last name of the singer")
	var albums []*Album
	// Join the Singer association to use it in the Where clause.
	// Note that `gorm` will use "Singer" (including quotes) as the alias for the singers table.
	// That means that all references to "Singer" in the query must be quoted, as PostgreSQL treats
	// the alias as case-sensitive.
	if err := db.Joins("Singer").Where(
		`lower(substring(albums.title, 1, 1)) = lower(substring("Singer".first_name, 1, 1))` +
			`or lower(substring(albums.title, 1, 1)) = lower(substring("Singer".last_name, 1, 1))`,
	).Order(`"Singer".last_name, "albums".release_date asc`).Find(&albums).Error; err != nil {
		fmt.Printf("Failed to load albums: %v\n", err)
		return
	}
	if len(albums) == 0 {
		fmt.Println("No albums found that match the criteria")
	} else {
		for _, album := range albums {
			fmt.Printf("Album %q was released by %v\n", album.Title, album.Singer.FullName)
		}
	}
	fmt.Print("\n\n")
}

// SearchAlbumsUsingNamedArgument searches for Albums using a named argument.
func SearchAlbumsUsingNamedArgument(db *gorm.DB, title string) {
	fmt.Printf("Searching for albums like %q\n", title)
	var albums []*Album
	if err := db.Where("title like @title", sql.Named("title", title)).Order("title").Find(&albums).Error; err != nil {
		fmt.Printf("Failed to load albums: %v\n", err)
		return
	}
	if len(albums) == 0 {
		fmt.Println("No albums found that match the criteria")
	} else {
		for _, album := range albums {
			fmt.Printf("Album %q released at %v\n", album.Title, time.Time(album.ReleaseDate).Format("2006-01-02"))
		}
	}
	fmt.Print("\n\n")
}

// CreateSinger creates a new Singer and stores in the database.
// Returns the ID of the Singer.
func CreateSinger(db *gorm.DB, firstName, lastName string) (string, error) {
	singer := Singer{
		BaseModel: BaseModel{ID: uuid.NewString()},
		FirstName: sql.NullString{String: firstName, Valid: true},
		LastName:  lastName,
	}
	res := db.Create(&singer)
	return singer.ID, res.Error
}

// CreateAlbumWithRandomTracks creates and stores a new Album in the database.
// Also generates numTracks random tracks for the Album.
// Returns the ID of the Album.
func CreateAlbumWithRandomTracks(db *gorm.DB, singerId, albumTitle string, numTracks int) (string, error) {
	albumId := uuid.NewString()
	// We cannot include the Tracks that we want to create in the definition here, as gorm would then try to
	// use an UPSERT to save-or-update the album that we are creating. Instead, we need to create the album first,
	// and then create the tracks.
	res := db.Create(&Album{
		BaseModel:       BaseModel{ID: albumId},
		Title:           albumTitle,
		MarketingBudget: decimal.NullDecimal{Decimal: decimal.NewFromFloat(randFloat64(0, 10000000))},
		ReleaseDate:     randDate(),
		SingerId:        singerId,
		CoverPicture:    randBytes(randInt(5000, 15000)),
	})
	if res.Error != nil {
		return albumId, res.Error
	}
	tracks := make([]*Track, numTracks)
	for n := 0; n < numTracks; n++ {
		tracks[n] = &Track{BaseModel: BaseModel{ID: albumId}, TrackNumber: int64(n + 1), Title: randTrackTitle(), SampleRate: randFloat64(30.0, 60.0)}
	}

	// Note: The batch size is deliberately kept small here in order to prevent the statement from getting too big and
	// exceeding the maximum number of parameters in a prepared statement. PGAdapter can currently handle at most 50
	// parameters in a prepared statement.
	res = db.CreateInBatches(tracks, 8)
	return albumId, res.Error
}

// DeleteRandomTrack will delete a randomly chosen Track from the database.
// This function shows how to delete a record with a primary key consisting of more than one column.
func DeleteRandomTrack(db *gorm.DB) {
	track := Track{}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&track).Error; err != nil {
			return err
		}
		if track.ID == "" {
			return fmt.Errorf("no track found")
		}
		if res := tx.Delete(&track); res.Error != nil || res.RowsAffected != int64(1) {
			if res.Error != nil {
				return res.Error
			}
			return fmt.Errorf("delete affected %d rows", res.RowsAffected)
		}
		return nil
	}); err != nil {
		fmt.Printf("Failed to delete a random track: %v\n", err)
		return
	}
	fmt.Printf("Deleted track %q (%q)\n\n", track.ID, track.Title)
}

// DeleteRandomAlbum deletes a random Album. The Album could have one or more Tracks interleaved with it, but as the
// `INTERLEAVE IN PARENT` clause includes `ON DELETE CASCADE`, the child rows will be deleted along with the parent.
func DeleteRandomAlbum(db *gorm.DB) {
	album := Album{}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&album).Error; err != nil {
			return err
		}
		if album.ID == "" {
			return fmt.Errorf("no album found")
		}
		// Note that the number of rows affected that is returned by Cloud Spanner excludes the number of child rows
		// that was deleted along with the parent row. This means that the number of rows affected should always be 1.
		if res := tx.Delete(&album); res.Error != nil || res.RowsAffected != int64(1) {
			if res.Error != nil {
				return res.Error
			}
			return fmt.Errorf("delete affected %d rows", res.RowsAffected)
		}
		return nil
	}); err != nil {
		fmt.Printf("Failed to delete a random album: %v\n", err)
		return
	}
	fmt.Printf("Deleted album %q (%q)\n\n", album.ID, album.Title)
}

// DeleteAllData deletes all existing records in the database.
func DeleteAllData(db *gorm.DB) {
	db.Exec("DELETE FROM concerts")
	db.Exec("DELETE FROM venues")
	db.Exec("DELETE FROM albums")
	db.Exec("DELETE FROM singers")
}

func randFloat64(min, max float64) float64 {
	return min + rnd.Float64()*(max-min)
}

func randInt(min, max int) int {
	return min + rnd.Int()%(max-min)
}

func randDate() datatypes.Date {
	return datatypes.Date(time.Date(randInt(1850, 2010), time.Month(randInt(1, 12)), randInt(1, 28), 0, 0, 0, 0, time.UTC))
}

func randBytes(length int) []byte {
	res := make([]byte, length)
	rnd.Read(res)
	return res
}

func randFirstName() string {
	return firstNames[randInt(0, len(firstNames))]
}

func randLastName() string {
	return lastNames[randInt(0, len(lastNames))]
}

func randAlbumTitle() string {
	return adjectives[randInt(0, len(adjectives))] + " " + nouns[randInt(0, len(nouns))]
}

func randTrackTitle() string {
	return adverbs[randInt(0, len(adverbs))] + " " + verbs[randInt(0, len(verbs))]
}

var firstNames = []string{
	"Saffron", "Eleanor", "Ann", "Salma", "Kiera", "Mariam", "Georgie", "Eden", "Carmen", "Darcie",
	"Antony", "Benjamin", "Donald", "Keaton", "Jared", "Simon", "Tanya", "Julian", "Eugene", "Laurence"}
var lastNames = []string{
	"Terry", "Ford", "Mills", "Connolly", "Newton", "Rodgers", "Austin", "Floyd", "Doherty", "Nguyen",
	"Chavez", "Crossley", "Silva", "George", "Baldwin", "Burns", "Russell", "Ramirez", "Hunter", "Fuller",
}
var adjectives = []string{
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
	"unruly",
}
var nouns = []string{
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
	"extent",
}
var verbs = []string{
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
	"remove",
}
var adverbs = []string{
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
	"queasily",
}

func parseTimestamp(ts string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, ts)
	return t.UTC()
}
