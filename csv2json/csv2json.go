package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)
func main() {
	// Showing useful information when the user enters the --help option
	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] <csvFile>\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}
	// Getting the file data that was entered by the user
	fileData, err := getFileData()

	if err != nil {
		exitGracefully(err)
	}
	// Validating the file entered
	if _, err := checkIfValidFile(fileData.filepath); err != nil {
		exitGracefully(err)
	}
	// Declaring the channels that our go-routines are going to use
	writerChannel := make(chan map[string]string)
	done := make(chan bool) 
	// Running both of our go-routines, the first one responsible for reading and the second one for writing
	go processCsvFile(fileData, writerChannel) 
	go writeJSONFile(fileData.filepath, writerChannel, done, fileData.pretty)
	// Waiting for the done channel to receive a value, so that we can terminate the programn execution
	<-done 
}

type inputFile struct {
	filepath  string
	separator string
	pretty    bool
}

func getFileData() (inputFile, error) {
	// We need to validate that we're getting the correct number of arguments
	if len(os.Args) < 2 {
		return inputFile{}, errors.New("a filepath argument is required")
	}

	// Defining option flags. For this, we're using the Flag package from the standard library
	// We need to define three arguments: the flag's name, the default value, and a short description (displayed whith the option --help)
	separator := flag.String("separator", "comma", "Column separator")
	pretty := flag.Bool("pretty", false, "Generate pretty JSON")

	flag.Parse() // This will parse all the arguments from the terminal

	fileLocation := flag.Arg(0) // The only argument (that is not a flag option) is the file location (CSV file)

	// Validating whether or not we received "comma" or "semicolon" from the parsed arguments.
	// If we dind't receive any of those. We should return an error
	if !(*separator == "comma" || *separator == "semicolon") {
		return inputFile{}, errors.New("only comma or semicolon separators are allowed")
	}

	// If we get to this endpoint, our programm arguments are validated
	// We return the corresponding struct instance with all the required data
	return inputFile{fileLocation, *separator, *pretty}, nil
}

func checkIfValidFile(filename string) (bool, error) {
	// Checking if entered file is CSV by using the filepath package from the standard library
	if fileExtension := filepath.Ext(filename); fileExtension != ".csv" {
		return false, fmt.Errorf("file %s is not CSV", filename)
	}

	// Checking if filepath entered belongs to an existing file. We use the Stat method from the os package (standard library)
	if _, err := os.Stat(filename); err != nil && os.IsNotExist(err) {
		return false, fmt.Errorf("file %s does not exist", filename)
	}
  	// If we get to this point, it means this is a valid file
	return true, nil
}

func processCsvFile(fileData inputFile, writerChannel chan<- map[string]string) {
	// Opening our file for reading
	file, err := os.Open(fileData.filepath)
  	// Checking for errors, we shouldn't get any
	check(err)
  	// Don't forget to close the file once everything is done
	defer file.Close()

	// Defining a "headers" and "line" slice
	var headers, line []string
	// Initializing our CSV reader 
	reader := csv.NewReader(file)
  	// The default character separator is comma (,) so we need to change to semicolon if we get that option from the terminal
	if fileData.separator == "semicolon" {
		reader.Comma = ';'
	}
  	// Reading the first line, where we will find our headers
	headers, err = reader.Read()
	check(err) // Again, error checking
  	// Now we're going to iterate over each line from the CSV file
	for {
		// We read one row (line) from the CSV.
		// This line is a string slice, with each element representing a column
		line, err = reader.Read()
		// If we get to End of the File, we close the channel and break the for-loop
		if err == io.EOF {
			close(writerChannel)
			break
		} else if err != nil {
			exitGracefully(err) // If this happens, we got an unexpected error
		}
		// Processiong a CSV line
		record, err := processLine(headers, line)

		if err != nil { // If we get an error here, it means we got a wrong number of columns, so we skip this line
			fmt.Printf("Line: %sError: %s\n", line, err)
			continue
		}
		// Otherwise, we send the processed record to the writer channel
		writerChannel <- record
	}
}

func processLine(headers []string, dataList []string) (map[string]string, error) {
	// Validating if we're getting the same number of headers and columns. Otherwise, we return an error
	if len(dataList) != len(headers) {
		return nil, errors.New("line doesn't match headers format. Skipping")
	}
	// Creating the map we're going to populate
	recordMap := make(map[string]string)
	// For each header we're going to set a new map key with the corresponding column value
	for i, name := range headers {
		recordMap[name] = dataList[i]
	}
	// Returning our generated map
	return recordMap, nil
}

func writeJSONFile(csvPath string, writerChannel <-chan map[string]string, done chan<- bool, pretty bool) {
	writeString := createStringWriter(csvPath) // Instanciating a JSON writer function
	jsonFunc, breakLine := getJSONFunc(pretty) // Instanciating the JSON parse function and the breakline character
	 // Log for informing
	fmt.Println("Writing JSON file...")
	// Writing the first character of our JSON file. We always start with a "[" since we always generate array of record
	writeString("["+breakLine, false) 
	first := true
	for {
		// Waiting for pushed records into our writerChannel
		record, more := <-writerChannel
		if more {
			if !first { // If it's not the first record, we break the line
				writeString(","+breakLine, false)
			} else {
				first = false // If it's the first one, we don't break the line
			}

			jsonData := jsonFunc(record) // Parsing the record into JSON
			writeString(jsonData, false) // Writing the JSON string with our writer function
		} else { // If we get here, it means there aren't more record to parse. So we need to close the file
			writeString(breakLine+"]", true) // Writing the final character and closing the file
			fmt.Println("Completed!") // Logging that we're done
			done <- true // Sending the signal to the main function so it can correctly exit out.
			break // Stoping the for-loop
		}
	}
}

func createStringWriter(csvPath string) func(string, bool) {
	jsonDir := filepath.Dir(csvPath) // Getting the directory where the CSV file is
	jsonName := fmt.Sprintf("%s.json", strings.TrimSuffix(filepath.Base(csvPath), ".csv")) // Declaring the JSON filename, using the CSV file name as base
	finalLocation := filepath.Join(jsonDir, jsonName) // Declaring the JSON file location, using the previous variables as base
	// Opening the JSON file that we want to start writing
	f, err := os.Create(finalLocation) 
	check(err)
	// This is the function we want to return, we're going to use it to write the JSON file
	return func(data string, close bool) { // 2 arguments: The piece of text we want to write, and whether or not we should close the file
		_, err := f.WriteString(data) // Writing the data string into the file
		check(err)
		// If close is "true", it means there are no more data left to be written, so we close the file
		if close { 
			f.Close()
		}
	}
}

func getJSONFunc(pretty bool) (func(map[string]string) string, string) {
	// Declaring the variables we're going to return at the end
	var jsonFunc func(map[string]string) string
	var breakLine string
	if pretty { //Pretty is enabled, so we should return a well-formatted JSON file (multi-line)
		breakLine = "\n"
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.MarshalIndent(record, "   ", "   ") // By doing this we're ensuring the JSON generated is indented and multi-line
			return "   " + string(jsonData) // Transforming from binary data to string and adding the indent characets to the front
		}
	} else { // Now pretty is disabled so we should return a compact JSON file (one single line)
		breakLine = "" // It's an empty string because we never break lines when adding a new JSON object
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.Marshal(record) // Now we're using the standard Marshal function, which generates JSON without formating
			return string(jsonData) // Transforming from binary data to string
		}
	}

	return jsonFunc, breakLine // Returning everythinbg
}

func exitGracefully(err error) {
	panic("unimplemented")
}

func check(e error) {
   if e != nil {
      exitGracefully(e)
   }
}

func Test_getFileData(t *testing.T) {
	// Defining our test slice. Each unit test should have the following properties:
	tests := []struct {
		name    string    // The name of the test
		want    inputFile // What inputFile instance we want our function to return.
		wantErr bool      // whether or not we want an error.
		osArgs  []string  // The command arguments used for this test
	}{
		// Here we're declaring each unit test input and output data as defined before
		{"Default parameters", inputFile{"test.csv", "comma", false}, false, []string{"cmd", "test.csv"}},
		{"No parameters", inputFile{}, true, []string{"cmd"}},
		{"Semicolon enabled", inputFile{"test.csv", "semicolon", false}, false, []string{"cmd", "--separator=semicolon", "test.csv"}},
		{"Pretty enabled", inputFile{"test.csv", "comma", true}, false, []string{"cmd", "--pretty", "test.csv"}},
		{"Pretty and semicolon enabled", inputFile{"test.csv", "semicolon", true}, false, []string{"cmd", "--pretty", "--separator=semicolon", "test.csv"}},
		{"Separator not identified", inputFile{}, true, []string{"cmd", "--separator=pipe", "test.csv"}},
	}
	// Iterating over the previous test slice
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Saving the original os.Args reference
			actualOsArgs := os.Args
			// This defer function will run after the test is done
			defer func() {
				os.Args = actualOsArgs                                           // Restoring the original os.Args reference
				flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Reseting the Flag command line. So that we can parse flags again
			}()

			os.Args = tt.osArgs             // Setting the specific command args for this test
			got, err := getFileData()       // Runing the function we want to test
			if (err != nil) != tt.wantErr { // Asserting whether or not we get the corret error value
				t.Errorf("getFileData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) { // Asserting whether or not we get the corret wanted value
				t.Errorf("getFileData() = %v, want %v", got, tt.want)
			}
		})
	}
}
