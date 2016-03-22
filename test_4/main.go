package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var outFile = flag.String("o", "./output.db", "specify output sqlite file")

// number of goroutines used to filer records (can be actually one higher because of rounding)
var goroutinesNumber = flag.Int("g", 4, "specify number of goroutines for parallel filtering")

const salesTable = "sales"
const createTableSql = "CREATE TABLE IF NOT EXISTS " + salesTable + " (id INTEGER PRIMARY KEY, Address TEXT, Suburb TEXT, Date DATE, Value INTEGER)"
const dateLayout = "1/2/06"

type record struct {
	id              int
	address, suburb string
	date            time.Time
	value           int
}

func (rec record) String() string {
	return fmt.Sprintf("%v, %v, %v, %v, %v", rec.id, rec.address, rec.suburb, rec.date, rec.value)
}

var emptyRecordError = errors.New("Empty record")

func NewRecordFromStrings(strings []string) (rec record, err error) {
	if isEmptyRecord(strings) {
		return rec, emptyRecordError
	}
	rec.id, err = strconv.Atoi(strings[0])
	if err != nil {
		return
	}
	rec.address, rec.suburb = strings[1], strings[2]

	rec.date, err = time.Parse(dateLayout, strings[3])
	if err != nil {
		return
	}
	rec.value, err = strconv.Atoi(strings[4])
	if err != nil {
		return
	}
	return
}

// returns true if all strings in a slice are empty strings
func isEmptyRecord(record []string) bool {
	for _, val := range record {
		if val != "" {
			return false
		}
	}
	return true
}

func loadAndDeduplicateRecords(in io.Reader) (recordsDedup []record, err error) {
	r := csv.NewReader(in)

	// skip the first line
	_, err = r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		} else {
			return
		}
	}

	records := []record{}
	duplicates := make(map[int]int) //[id]inserts_num
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		rec, err := NewRecordFromStrings(record)
		if err != nil {
			if err == emptyRecordError {
				continue
			} else {
				return nil, err
			}
		}
		records = append(records, rec)
		inserts := duplicates[rec.id]
		inserts++
		duplicates[rec.id] = inserts
	}
	recordsDedup = filterDuplicates(records, duplicates)
	return
}

func processFile(in io.Reader, tx *sql.Tx) error {
	recordsDedup, err := loadAndDeduplicateRecords(in)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO " + salesTable + " VALUES(?, ?, ?, ?, ?);")
	if err != nil {
		return err
	}
	defer stmt.Close()

	var wg sync.WaitGroup
	ch := make(chan record)

	chunkSize := max(len(recordsDedup)/(*goroutinesNumber), 1)
	dedupEnd := len(recordsDedup)
	var chunkStart int
	chunkEnd := min(chunkSize, dedupEnd)
	for {
		chunk := recordsDedup[chunkStart:chunkEnd]
		wg.Add(1)
		go func() {
			filtered := filter(chunk)
			for _, rec := range filtered {
				ch <- rec
			}
			wg.Done()
		}()

		if chunkEnd == dedupEnd {
			break
		}
		chunkStart = chunkEnd
		chunkEnd = min(chunkEnd+chunkSize, dedupEnd)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		doneCh <- struct{}{}
	}()
	var done bool
	for !done {
		select {
		case rec := <-ch:
			_, err := stmt.Exec(rec.id, rec.address, rec.suburb, rec.date, rec.value)
			if err != nil {
				return err
			}
		case <-doneCh:
			done = true
		}
	}

	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// filterDuplicates filters out records inserted more than once
// parameter inserts is a map[id]n_inserts
func filterDuplicates(records []record, inserts map[int]int) []record {
	var output []record
	for _, rec := range records {
		if inserts[rec.id] == 1 {
			output = append(output, rec)
		}
	}
	return output
}

func filter(records []record) []record {
	var out []record
	var i int
	for _, rec := range records {
		if rec.value < 400000 {
			continue
		}
		addrTrim := strings.TrimSpace(rec.address)
		if strings.HasSuffix(addrTrim, "AVE") ||
			strings.HasSuffix(addrTrim, "CRES") ||
			strings.HasSuffix(addrTrim, "PL") {
			continue
		}
		i++
		if i == 10 {
			i = 0
			continue
		}
		out = append(out, rec)
	}
	return out
}

func printSalesTable(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM " + salesTable)
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Content of sales table: ")
	for rows.Next() {
		var id int
		var address, suburb, value string
		var date time.Time
		if err := rows.Scan(&id, &address, &suburb, &date, &value); err != nil {
			return err
		}
		fmt.Printf("%v, %v, %v, %v, %v\n", id, address, suburb, date, value)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: $ %s file1.csv file2.csv ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	*goroutinesNumber = max(*goroutinesNumber, 1)

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	db, err := sql.Open("sqlite3", *outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(createTableSql)
	if err != nil {
		log.Fatal(err)
	}

	for _, filename := range flag.Args() {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		if err := processFile(file, tx); err != nil {
			file.Close()
			tx.Rollback()
			log.Fatal(err)
		} else {
			tx.Commit()
			file.Close()
		}
	}

	if err := printSalesTable(db); err != nil {
		log.Fatal(err)
	}
}
