package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

var outFile = flag.String("o", "./output.db", "specify output sqlite file")

const salesTable = "sales"
const createTableSql = "CREATE TABLE IF NOT EXISTS " + salesTable + " (id INTEGER PRIMARY KEY, Address TEXT, Suburb TEXT, Date DATE, Value TEXT)"
const dateLayout = "1/2/06"

// returns true if all strings in a slice are empty strings
func isEmptyRecord(record []string) bool {
	for _, val := range record {
		if val != "" {
			return false
		}
	}
	return true
}

func processFile(in io.Reader, tx *sql.Tx) error {
	r := csv.NewReader(in)

	// skip the first line
	_, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil
		} else {
			return err
		}
	}

	// insert values. does nothing, if id already exists
	stmtInsert, err := tx.Prepare("INSERT OR IGNORE INTO " + salesTable + " VALUES(?, ?, ?, ?, ?);")
	if err != nil {
		return err
	}
	defer stmtInsert.Close()

	// update values in case the date is newer or equal
	stmtUpdate, err := tx.Prepare("UPDATE " + salesTable + " SET address=?, suburb=?, Date=?, Value=? WHERE id=? AND Date<=?")
	if err != nil {
		return err
	}
	defer stmtUpdate.Close()

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		if isEmptyRecord(record) {
			continue
		}

		id, err := strconv.ParseInt(record[0], 0, 64)
		if err != nil {
			return err
		}
		date, err := time.Parse(dateLayout, record[3])
		if err != nil {
			return err
		}

		address, suburb, value := record[1], record[2], record[4]
		_, err = stmtInsert.Exec(id, address, suburb, date, value)
		if err != nil {
			return err
		}
		_, err = stmtUpdate.Exec(address, suburb, date, value, id, date)
		if err != nil {
			return err
		}
	}
	return nil
}

func printSalesTable(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM " + salesTable)
	if err != nil {
		return err
	}
	defer rows.Close()

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
