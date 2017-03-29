package main

import (
	//"fmt"

	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli"
	"gopkg.in/rana/ora.v3"
	//	"unicode/utf8"
	"bufio"
)

var user, password, dblink, nlsDateFmt, nlsNumericCh, nullText string
var comma string
var bindRows, maxErrors, cntErrors int64
var bulk bool
var sql, sqlBegin, sqlEnd cli.StringSlice

func str2orastr(rec []string) []ora.String {
	a := make([]ora.String, len(rec))
	for i, val := range rec {
		if val == "" {
			a[i] = ora.String{IsNull: true}
		} else {
			a[i] = ora.String{Value: val}
		}
	}
	return a
}
func main() {

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "user, u", Destination: &user, EnvVar: "ORADB_USER"},
		cli.StringFlag{Name: "password, p", Destination: &password, EnvVar: "ORADB_PASSWORD"},
		cli.StringFlag{Name: "connect, c", Destination: &dblink, EnvVar: "ORADB_CONNECT", Usage: "example: \"host:port/service\""},
		cli.StringFlag{Name: "nlsDate", Destination: &nlsDateFmt, EnvVar: "NLS_DATE_FORMAT", Usage: "example: \"DD.MM.YYYY\""},
		cli.StringFlag{Name: "nlsNumeric", Destination: &nlsNumericCh, EnvVar: "NLS_NUMERIC_CHARACTERS", Usage: "example: \".,\""},
		cli.Int64Flag{Name: "bindRows", Destination: &bindRows, Value: 100000, Usage: "example: 10000"},
		cli.Int64Flag{Name: "maxErrors", Destination: &maxErrors, Value: 0, Usage: "example: 10"},
		cli.BoolFlag{Name: "bulk", Destination: &bulk, Value: true, Usage: "example: --bulk"},
		cli.StringFlag{Name: "delimiter, d", Value: ",", Destination: &comma, Usage: "example: \"\\t\" for tab "},
		cli.StringFlag{Name: "nullText", Value: "", Destination: &nullText, Usage: "The value is textual representation of oracle null values"},
		cli.StringSliceFlag{Name: "exec, e", Value: &sql, Usage: "The command executed for each line of input: -e \"insert into TAB1 values(:v1, :v2)\""},
		cli.StringSliceFlag{Name: "begin", Value: &sqlBegin, Usage: "The command executed at the begining: --begin \"delete from TAB1\""},
		cli.StringSliceFlag{Name: "end", Value: &sqlEnd, Usage: "The command executed at the end: --end \"delete from TAB1 where col1 is null\""},
	}
	app.Action = func(c *cli.Context) error {
		env, err := ora.OpenEnv(nil)
		defer env.Close()
		if err != nil {
			panic(err)
		}

		srvcfg := &ora.SrvCfg{Dblink: dblink}
		srv, err := env.OpenSrv(srvcfg)
		defer srv.Close()
		if err != nil {
			panic(err)
		}
		sesCfg := ora.SesCfg{
			Username: user,
			Password: password,
		}
		ses, err := srv.OpenSes(&sesCfg)
		defer ses.Close()
		if err != nil {
			panic(err)
		}
		if len(sql) == 0 {
			// pokud nemame select ani cursor z prikazove radky, ocekavame select z Stdin
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				sql = append(sql, scanner.Text())
			}
			if err = scanner.Err(); err != nil {
				log.Fatalln("Error reading standard input:", err)
			}
		}
		if len(sql) == 0 {
			// chybi select nebo cursor na vstupu
			log.Fatalln("No SQL command to execute")
		}

		sqlstmt := strings.Join(sql, " ")
		// fmt.Println(sqlstmt)
		stmt, err := ses.Prep(sqlstmt)
		if err != nil {
			log.Panicf("%s %v", "Error parsing "+sqlstmt, err)
		}
		defer stmt.Close()

		if nlsDateFmt != "" {
			_, err2 := ses.PrepAndExe(fmt.Sprintf("alter session set nls_date_format='%s'", nlsDateFmt))
			if err2 != nil {
				log.Panicf("%s %v", nlsDateFmt, err2)
			}
		}
		if nlsNumericCh != "" {
			ses.PrepAndExe(fmt.Sprintf("alter session set nls_numeric_characters='%s'", nlsNumericCh))
		}

		for _, sql := range sqlBegin {
			_, err2 := ses.PrepAndExe(sql)
			if err2 != nil {
				log.Panicf("%s %v", sql, err2)
			}
		}

		//podle poctu parametru budeme mit velikost pole pro Execute
		numParams := stmt.NumInput()
		batch := make([]interface{}, numParams)
		for i := range batch {
			batch[i] = []ora.String{}
		}

		for _, file := range c.Args() {
			ior, err2 := os.Open(file)
			if err2 != nil {
				log.Fatal("error opening file ", err2)
			}
			csvr := csv.NewReader(ior)
			var batchSize int64
			for {
				rec, err2 := csvr.Read()
				if err2 == io.EOF {
					break
				}
				if err2 != nil {
					log.Fatal("Error reading file "+file, err2)
				}
				batchSize++
				for i, val := range rec {
					if i < numParams {
						if val == "" || val == nullText {
							batch[i] = append(batch[i].([]ora.String), ora.String{IsNull: true})
						} else {
							batch[i] = append(batch[i].([]ora.String), ora.String{Value: val})
						}
					}
				}
				if batchSize == bindRows {

					if len(batch) > 0 && len(batch[0].([]ora.String)) > 0 {
						rows, err := stmt.Exe(batch...)
						if err != nil {
							cntErrors++
							if cntErrors > maxErrors {
								log.Fatalln(err)
							} else {
								fmt.Println(err)
								if batchSize = 1 {
									fmt.Printf("%v\n", rec)
								}
						}
						fmt.Printf("%d rows", rows)
						for i := range batch {
							batch[i] = []ora.String{}
						}
						batchSize = 0
					}

				}

				// fmt.Printf("%v\n", rec)
			}
		}

		// fmt.Printf("%v\n", batch)
		if len(batch) > 0 && len(batch[0].([]ora.String)) > 0 {
			rows, err := stmt.Exe(batch...)
			if err != nil {
				panic(err)
			}
			fmt.Println(rows)
		}
		for _, sql := range sqlEnd {
			_, err2 := ses.PrepAndExe(sql)
			if err2 != nil {
				log.Panicf("%s %v", sql, err2)
			}
		}
		return nil
	}
	app.Run(os.Args)
}
