package main

import (
	"encoding/hex"
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/barasher/go-exiftool"
	"golang.org/x/crypto/sha3"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type ProgramOptions struct {
	DebugMode            bool     `json:"debug_mode"`
	UtcOffsetHours       int      `json:"utc_offset_hours"`
	FilenameExtension    string   `json:"filename_extension"`
	QueueLength          int      `json:"queue_length"`
	SourceDirs           []string `json:"source_dirs"`
	DestinationLocations []string `json:"destination_dirs"`
}

type RawfileInfo struct {
	Paths                   PathInfo
	BaseFilename            string
	FilesizeBytes           int
	Timestamp               time.Time
	OutputRelativeDirectory string
	OutputRelativePath      string
}

type PathInfo struct {
	AbsolutePath string
	RelativePath string
}

type EnumerationChannelInfo struct {
	sourcedir  string
	foundFiles []RawfileInfo
}

type FileDateTimeChannelRequest struct {
	sourcedirIndex int
	absolutePath   string
}

type FileDateTimeChannelEntry struct {
	sourcedirIndex int
	fileDateTime   time.Time
}

type FileCopierRawfileInfo struct {
	RelativePath    string
	BaseFilename    string
	Timestamp       time.Time
	AbsolutePaths   []string
	DestinationDirs []string
}

func parseArgs() ProgramOptions {
	parser := argparse.NewParser("", "Photo end of day script")
	debugMode := parser.Flag("", "debug", &argparse.Options{
		Required: false,
		Help:     "Enable debug output",
		Default:  false,
	})

	optionalSourcedirs := parser.StringList("", "additional_sourcedir", &argparse.Options{
		Required: false,
		Help:     "If there are more than one sourcedir to read from",
		Default:  nil,
	})

	timestampUtcOffsetHours := parser.Int("", "timestamp_utc_offset_hours", &argparse.Options{
		Required: false,
		Help:     "Hours offset from UTC",
		Default:  0,
	})

	requiredSourcedir := parser.StringPositional(nil)
	filenameExtension := parser.SelectorPositional([]string{"nef", "cr3"}, nil)
	destinationLocation := parser.StringPositional(nil)

	// Parse the options
	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	sourceDirs := []string{*requiredSourcedir}
	if optionalSourcedirs != nil {
		sourceDirs = append(sourceDirs, *optionalSourcedirs...)
	}

	destinationDirs := []string{*destinationLocation}

	programOpts := ProgramOptions{
		DebugMode:            *debugMode,
		UtcOffsetHours:       *timestampUtcOffsetHours,
		FilenameExtension:    *filenameExtension,
		SourceDirs:           sourceDirs,
		DestinationLocations: destinationDirs,
	}

	//jsonBytes, err := json.MarshalIndent(programOpts, "", "    ")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("Options:\n%s\n", string(jsonBytes))

	return programOpts
}

func iterateSourcedirFiles(sourceDir string, programOpts ProgramOptions,
	enumeratedFilesChannel chan EnumerationChannelInfo) {

	var foundFiles []RawfileInfo

	err := filepath.Walk(sourceDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// if it's a directory, skip it
			if info.IsDir() {
				return nil
			}

			// Make sure it ends with proper extension
			if !strings.HasSuffix(strings.ToLower(info.Name()), "."+programOpts.FilenameExtension) {
				return nil
			}

			newRawfile := RawfileInfo{
				Paths: PathInfo{
					path,
					strings.TrimPrefix(path, sourceDir+"\\"),
				},
				FilesizeBytes: int(info.Size()),
				BaseFilename:  info.Name(),
			}
			foundFiles = append(foundFiles, newRawfile)
			return nil
		})

	if err != nil {
		panic(err)
	}

	// send collected list of files and then bail, we've done our job
	enumeratedFilesChannel <- EnumerationChannelInfo{
		sourceDir,
		foundFiles,
	}
}

func confirmIdenticalFilelists(foundFiles map[string][]RawfileInfo) bool {
	identicalCheck := make(map[string][]int)

	// Walk through every list and add an entry from its relative type to its filesize
	for _, fileList := range foundFiles {
		for _, currRawfileInfo := range fileList {
			// Do we have an entry for this file yet?
			currArray, ok := identicalCheck[currRawfileInfo.Paths.RelativePath]
			if !ok {
				identicalCheck[currRawfileInfo.Paths.RelativePath] = make([]int, 2)
			}
			currArray = append(currArray, currRawfileInfo.FilesizeBytes)
		}
	}

	// To compare we're identical, all entries in the map should have two filesize values that are the same
	for relativeFilePath, identicalCheckEntry := range identicalCheck {
		if len(identicalCheckEntry) != 2 {
			log.Printf("Relative key %s does not have two file entries\n", relativeFilePath)
			return false
		}

		if identicalCheckEntry[0] != identicalCheckEntry[1] {
			log.Printf("File bytes for %s do not match", relativeFilePath)
			return false
		}
	}

	return true
}

func parseExifDate(exifDateTime string) time.Time {
	var year int
	var month int
	var day int
	var hour int
	var minute int
	var second int
	const ns int = 0
	timeLoc := time.UTC

	//fmt.Printf("Got exif datetime %s, parsing with sScanf\n", exifDateTime)

	_, err := fmt.Sscanf(exifDateTime, "%d:%d:%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second)

	//fmt.Println("Back from scanf")

	if err != nil {
		panic(err)
	}

	returnTime := time.Date(year, time.Month(month), day, hour, minute, second, ns, timeLoc)

	return returnTime
}

func iso8601Datetime(timeToFormat time.Time) string {
	return timeToFormat.Format(time.DateTime + "Z")
}

func getRawfileDateTimeWorker(incomingSourcefiles chan FileDateTimeChannelRequest,
	responseChan chan FileDateTimeChannelEntry) {

	et, err := exiftool.NewExiftool()
	if err != nil {
		fmt.Printf("Error when initializing ExifTool: %v\n", err)
		return
	}
	defer et.Close()

	// "range" will iterate reading over the channel until the channel is empty and closed
	for currDateTimeRequest := range incomingSourcefiles {
		//fmt.Printf("\tGot incoming file index %5d, path: %s, \n", currDateTimeRequest.sourcedirIndex,
		//	currDateTimeRequest.absolutePath)

		rawFileInfo := et.ExtractMetadata(currDateTimeRequest.absolutePath)

		//fmt.Printf("File %s has %d sections of info\n", currDateTimeRequest.absolutePath, len(rawFileInfo))

		// Read all sections of rawfile info
		for _, currRawfileInfoEntry := range rawFileInfo {
			if currRawfileInfoEntry.Err != nil {
				fmt.Printf("Error concerning %v: %v\n", currRawfileInfoEntry.File, currRawfileInfoEntry.Err)
			}

			// Make sure we have DateTimeOriginal, or shit is fuck
			if val, ok := currRawfileInfoEntry.Fields["DateTimeOriginal"]; ok {
				fileDateTime := parseExifDate(val.(string))
				//fmt.Printf("\tFile index %5d has extracted time %s\n",
				//	currDateTimeRequest.sourcedirIndex,
				//	iso8601Datetime(fileDateTime))

				// Send the date/time info back through the response channel
				extractedDatetime := FileDateTimeChannelEntry{
					currDateTimeRequest.sourcedirIndex,
					fileDateTime}
				responseChan <- extractedDatetime
				//fmt.Println("Send extracted datetime back through response channel")
			} else {
				fmt.Printf("Field 'DateTimeOriginal' is missing\n")
			}
		}
	}
}

func getRawfileDateTime(sourcefileList []RawfileInfo) {
	fmt.Println("\nGetting date/time of RAW files")

	sourcefilesNeedingDatetime := make(chan FileDateTimeChannelRequest)
	extractedDatesChannel := make(chan FileDateTimeChannelEntry)

	for range runtime.NumCPU() {
		// Launch goroutines to run Exiftool
		go getRawfileDateTimeWorker(sourcefilesNeedingDatetime, extractedDatesChannel)
	}

	datesToRead := len(sourcefileList)
	go func() {
		for i, currFileFromList := range sourcefileList {
			//fmt.Printf("\tFound file in list %s\n", currFileFromList.Paths.AbsolutePath)
			// Send (array index, sourcefile path) tuples to worker channel
			sourcefilesNeedingDatetime <- FileDateTimeChannelRequest{
				i,
				currFileFromList.Paths.AbsolutePath}
		}

		close(sourcefilesNeedingDatetime)
	}()

	// Read any remaining tuples from the response channel
	for i := datesToRead; i > 0; i-- {
		extractedDateInfo := <-extractedDatesChannel

		// Populate the time field of the incoming array at the specified index
		sourcefileList[extractedDateInfo.sourcedirIndex].Timestamp = extractedDateInfo.fileDateTime
	}

	fmt.Printf("\tDates extracted successfully from all %d rawfiles\n", len(sourcefileList))
}

func imageFileCopyWorker(inputFileInfo FileCopierRawfileInfo, wg *sync.WaitGroup) {
	fmt.Printf("\tWork starting on relative path %s\n",
		inputFileInfo.RelativePath)

	// Map from computed checksum to number of input copies with that checksum
	checksumsFound := make(map[string]int)

	fi, err := os.Stat(inputFileInfo.AbsolutePaths[0])
	if err != nil {
		panic("Could not get file info")
	}
	numberOfBytes := fi.Size()

	canonicalFileBytes := make([]byte, numberOfBytes)

	for i, currInputAbsolutePath := range inputFileInfo.AbsolutePaths {
		//fmt.Printf("\t\tChecksumming absolute path %s\n", currInputAbsolutePath)
		fileBytes, err := os.ReadFile(currInputAbsolutePath)
		if err != nil {
			panic("Could not read input file")
		}
		computedChecksum := sha3.Sum512(fileBytes)
		hexChecksum := hex.EncodeToString(computedChecksum[:])
		//fmt.Printf("\t\t\tGot hex checksum %s\n", hexChecksum)
		// Add to map of hashes we've seen
		val, ok := checksumsFound[hexChecksum]
		if !ok {
			checksumsFound[hexChecksum] = 1
		} else {
			checksumsFound[hexChecksum] = val + 1
		}

		// Store a copy of these file bytes away for copies should checksums match
		if i == 0 {
			copy(canonicalFileBytes, fileBytes)
		}
	}

	// Make sure we only got one input checksum, or we're bailing
	//fmt.Printf("\t\tChecksums seen: %v\n", checksumsFound)
	if len(checksumsFound) != 1 {
		panic("Inconsistent checksums for file " + inputFileInfo.RelativePath)
	}
	fmt.Println("\t\tAll checksums matched, can proceed with copy logic")

	// TODO: determine unique dest filename

	// TODO: iterate over dest dirs
	//		writeAttempts = 3
	//		successfulWrite = false
	//		while writeAttempts > 0 {
	//			write bytes to dest file
	//			close file
	//			flush file
	//			read file back
	//			if byte identical to the input array, mark successful = true and break
	//			else throw warn and try again
	//		}
	//		if successfulWrite is STILL false, panic because we can't do our work

	// Note that the work for this input file is complete and the function can terminate cleanly
	wg.Done()

	fmt.Printf("\tWork complete on relative path %s\n",
		inputFileInfo.RelativePath)

}

func main() {
	programOpts := parseArgs()
	//_ = parseArgs()

	// Create channel for the enumeration goroutines to write their file lists back to main
	filelistResultsChannel := make(chan EnumerationChannelInfo)

	fmt.Println("Enumerating sourcedirs")

	// For each sourcedir, spin off a goroutine to enumerate the files under that directory
	for _, currSourcedir := range programOpts.SourceDirs {
		go iterateSourcedirFiles(currSourcedir, programOpts, filelistResultsChannel)
	}

	foundFiles := make(map[string][]RawfileInfo)

	// Read out of the channel until we hit as many "end of files" markers as we have sourcedirs
	numdirs := len(programOpts.SourceDirs)

	for i := 0; i < numdirs; i++ {
		fileListFromChild := <-filelistResultsChannel

		foundFiles[fileListFromChild.sourcedir] = fileListFromChild.foundFiles
	}

	// Close the channel the children used, as they both signaled they are done writing
	close(filelistResultsChannel)

	for _, currSourcedir := range programOpts.SourceDirs {
		currFileList := foundFiles[currSourcedir]
		fmt.Printf("\tGot file list for \"%s\" with %d \".%s\" files\n", currSourcedir, len(currFileList),
			programOpts.FilenameExtension)
	}

	fmt.Println("\nValidating identical metadata for all sourcedirs")
	// Make sure all file lists are identical
	if confirmIdenticalFilelists(foundFiles) == false {
		panic("File lists are not identical")
	}

	fmt.Println("\tAll sourcedirs have identical metadata!")

	// Use Exiftool to pull date/time info from the RAW file
	//		NOTE: we're using the fact that the array is passed by reference, because the target
	//			function updates fields in each element of the array we pass down into this function
	getRawfileDateTime(foundFiles[programOpts.SourceDirs[0]])

	wg := &sync.WaitGroup{}

	fmt.Println("\nStarting file checksumming/copying operations")
	// Fire off goroutines to process each relative file
	for currFileIndex := range len(foundFiles[programOpts.SourceDirs[0]]) {
		currFileToPopulate := foundFiles[programOpts.SourceDirs[0]][currFileIndex]

		inputFileAbsolutePaths := make([]string, len(programOpts.SourceDirs))
		for i, currSourceDir := range programOpts.SourceDirs {
			inputFileAbsolutePaths[i] = foundFiles[currSourceDir][currFileIndex].Paths.AbsolutePath
		}

		inputFileInfo := FileCopierRawfileInfo{
			currFileToPopulate.Paths.RelativePath,
			currFileToPopulate.BaseFilename,
			currFileToPopulate.Timestamp,
			inputFileAbsolutePaths,
			programOpts.DestinationLocations}

		go imageFileCopyWorker(inputFileInfo, wg)
		wg.Add(1)

		break
	}

	wg.Wait()

	fmt.Println("\nAll file copies have been made and verified!")
}
