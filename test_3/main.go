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

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO " + salesTable + " VALUES(?, ?, ?, ?, ?);")
	if err != nil {
		return err
	}
	defer stmt.Close()

	duplicates := make(map[int64]struct{})
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
		res, err := stmt.Exec(id, address, suburb, date, value)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			duplicates[id] = struct{}{}
		}
	}

	if err := removeSalesWithIds(duplicates, tx); err != nil {
		return err
	}

	return nil
}

func removeSalesWithIds(ids map[int64]struct{}, tx *sql.Tx) error {
	stmt, err := tx.Prepare("DELETE FROM " + salesTable + " WHERE id=?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for id, _ := range ids {
		_, err := stmt.Exec(id)
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
