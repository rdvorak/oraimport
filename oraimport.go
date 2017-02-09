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

var user, password, dblink, dateFormat, nullText string
var comma string
var header, useCRLF bool
var sql, inputFile cli.StringSlice

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
		cli.StringFlag{Name: "connect, c", Destination: &dblink, EnvVar: "ORADB_CONNECT", Usage: "example: \"localhost:1524/MIS.OK.AERO\""},
		cli.StringFlag{Name: "delimiter, d", Value: ",", Destination: &comma, Usage: "example: \"\\t\" for tab "},
		cli.StringFlag{Name: "nullText", Value: "", Destination: &nullText, Usage: "The value is textual representation of oracle null values"},
		cli.StringSliceFlag{Name: "exec, e", Value: &sql, Usage: "The command executed for each line of input: -e \"insert into TAB1 values(:v1, :v2)\""},
		cli.StringSliceFlag{Name: "input, i", Value: &inputFile, Usage: "Input file"},
		cli.StringFlag{Name: "dateFormat", Value: "2006-01-02T15:04:05", Destination: &dateFormat, Usage: "format is the desired textual representation of the reference time: Mon Jan 2 15:04:05 -0700 MST 2006"},
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
		fmt.Println(sqlstmt)
		stmt, err := ses.Prep(sqlstmt)
		if err != nil {
			log.Fatalln(sqlstmt, err)
		}
		defer stmt.Close()
		numParams := stmt.NumInput()
		batch := make([]interface{}, numParams)
		for i := range batch {
			batch[i] = []ora.String{}
		}
		for _, file := range inputFile {
			ior, err := os.Open(file)
			if err != nil {
				log.Fatal("error opening file ", err)
			}
			csvr := csv.NewReader(ior)
			for {
				rec, err := csvr.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				for i, val := range rec {
					if i < numParams {
						if val == "" {
							batch[i] = append(batch[i].([]ora.String), ora.String{IsNull: true})
						} else {
							batch[i] = append(batch[i].([]ora.String), ora.String{Value: val})
						}
					}
				}
				// fmt.Printf("%v\n", rec)
			}
		}

		// fmt.Printf("%v\n", batch)
		rows, err := stmt.Exe(batch...)
		if err != nil {
			panic(err)
		}
		fmt.Println(rows)

		return nil
	}
	app.Run(os.Args)
}
