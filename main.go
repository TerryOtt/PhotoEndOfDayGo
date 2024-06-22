package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/barasher/go-exiftool"
	"github.com/tkrajina/gpxgo/gpx"
	"golang.org/x/crypto/sha3"
	"log"
	"os"
	"os/exec"
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
	GpxFile              *os.File `json:"gpx_file"`
	SourceDirs           []string `json:"source_dirs"`
	DestinationLocations []string `json:"destination_dirs"`
}

type RawfileInfo struct {
	Paths                     PathInfo
	BaseFilename              string
	FileExtension             string
	FilesizeBytes             int
	Timestamp                 time.Time
	OutputRelativeDirectory   string
	OutputRelativePath        string
	GeotaggedLocation         gpx.Point
	absoluteXmpFileWithGeotag string
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
	FileExtension   string
	Timestamp       time.Time
	AbsolutePaths   []string
	DestinationDirs []string
}

type FileContentsAndChecksumPair struct {
	absolutePath string
	fileContents []byte
	fileChecksum string
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

	gpxFile := parser.File("g", "gpxfile", 0, 0444, &argparse.Options{
		Required: false,
		Help:     "Optional GPX file to geotag RAW files",
		Default:  nil,
	})

	requiredSourcedir := parser.StringPositional(&argparse.Options{
		Required: true,
		Help:     "Required input directory",
		Default:  nil,
	})
	filenameExtension := parser.SelectorPositional([]string{"nef", "cr3"}, &argparse.Options{
		Required: true,
		Help:     "Rawfile image extension",
		Default:  nil,
	})
	destinationLocation := parser.StringPositional(&argparse.Options{
		Required: true,
		Help:     "Required output directory",
		Default:  nil,
	})

	optDest1 := parser.StringPositional(&argparse.Options{
		Required: false,
		Help:     "Optional output dir",
		Default:  "",
	})

	optDest2 := parser.StringPositional(&argparse.Options{
		Required: false,
		Help:     "Optional output dir",
		Default:  "",
	})

	optDest3 := parser.StringPositional(&argparse.Options{
		Required: false,
		Help:     "Optional output dir",
		Default:  "",
	})

	optDest4 := parser.StringPositional(&argparse.Options{
		Required: false,
		Help:     "Optional output dir",
		Default:  "",
	})

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
	if *optDest1 != "" {
		destinationDirs = append(destinationDirs, *optDest1)
	}
	if *optDest2 != "" {
		destinationDirs = append(destinationDirs, *optDest2)
	}

	if *optDest3 != "" {
		destinationDirs = append(destinationDirs, *optDest3)
	}

	if *optDest4 != "" {
		destinationDirs = append(destinationDirs, *optDest4)
	}

	programOpts := ProgramOptions{
		DebugMode:            *debugMode,
		UtcOffsetHours:       *timestampUtcOffsetHours,
		FilenameExtension:    *filenameExtension,
		GpxFile:              gpxFile,
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

			currFilename := info.Name()
			currFileExtension := filepath.Ext(currFilename)

			newRawfile := RawfileInfo{
				Paths: PathInfo{
					path,
					strings.TrimPrefix(path, sourceDir+"\\"),
				},
				FilesizeBytes: int(info.Size()),
				BaseFilename:  strings.TrimSuffix(currFilename, filepath.Ext(currFilename)),
				FileExtension: currFileExtension}
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

func confirmIdenticalFilelists(foundFiles map[string][]RawfileInfo, timer *PerfTimer) bool {
	defer timer.exitFunction(timer.enterFunction("Confirm source file lists are identical"))

	fmt.Println("\nValidating identical metadata for all sourcedirs")

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

	fmt.Println("\tAll sourcedirs have identical metadata!")
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

func getSizeOfOneCopyInBytes(sourcefileList []RawfileInfo, timer *PerfTimer) int64 {
	defer timer.exitFunction(timer.enterFunction("Computing total filesize of RAW images"))
	fmt.Println("\nComputing total bytes in this set of RAW images")
	cumulativeBytesInOneCopy := int64(0)

	for _, currSource := range sourcefileList {
		// Get size of this file in bytes
		if fileInfo, err := os.Stat(currSource.Paths.AbsolutePath); err == nil {
			cumulativeBytesInOneCopy += fileInfo.Size()
		}
	}

	fmt.Printf("\tSize of RAW files: %.01f GB\n", float64(cumulativeBytesInOneCopy)/(1024*1024*1024))

	return cumulativeBytesInOneCopy
}

func getRawfileDateTime(sourcefileList []RawfileInfo,
	timer *PerfTimer) {

	defer timer.exitFunction(timer.enterFunction("Extracting date info from RAW files"))

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

func readFileBytesAndChecksum(filePath string, responseChan chan FileContentsAndChecksumPair) {
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		panic("Could not read input file")
	}
	computedChecksum := sha3.Sum256(fileBytes)
	hexChecksum := hex.EncodeToString(computedChecksum[:])
	responseChan <- FileContentsAndChecksumPair{
		filePath,
		fileBytes,
		hexChecksum,
	}
}
func writeFileContents(canonicalFileBytes []byte, currDestfilePath string, wg *sync.WaitGroup) {
	successfulWrite := false
	// Create absolute path, open file for binary writing

	// Get absolute DIRECTORY path, and ensure all the directories are created that are needed
	dirPath := filepath.Dir(currDestfilePath)
	//fmt.Printf("\t\t\tDirectory path to file: %s\n", dirPath)
	os.MkdirAll(dirPath, 0777)

	for writeAttempts := 3; writeAttempts > 0; writeAttempts-- {
		if err := os.WriteFile(currDestfilePath, canonicalFileBytes, 0600); err != nil {
			continue
		}
		// read file back
		readbackBytes, err := os.ReadFile(currDestfilePath)
		if err != nil {
			continue
		}

		// Compare our readback bytes to what we wrote
		if bytes.Equal(canonicalFileBytes, readbackBytes) == true {
			successfulWrite = true
			break
		}
	}

	if successfulWrite == false {
		panic("Could not successfully write file")
	}

	wg.Done()
}

func imageFileCopyWorker(workerChannel chan FileCopierRawfileInfo, wg *sync.WaitGroup) {

	for inputFileInfo := range workerChannel {

		//fmt.Printf("\tWork starting on relative path %s\n",
		//	inputFileInfo.RelativePath)

		// Map from computed checksum to number of input copies with that checksum
		checksumsFound := make(map[string]int)

		// Fire off file read and checksum computes for all sourcedir versions
		//		Note that the channel is buffered, so writers will not block
		channelCapacity := max(len(inputFileInfo.AbsolutePaths), len(inputFileInfo.DestinationDirs))
		contentsChecksumChan := make(chan FileContentsAndChecksumPair, channelCapacity)

		for _, currSourcefile := range inputFileInfo.AbsolutePaths {
			go readFileBytesAndChecksum(currSourcefile, contentsChecksumChan)
		}

		var bytesAndChecksum FileContentsAndChecksumPair
		for range len(inputFileInfo.AbsolutePaths) {
			bytesAndChecksum = <-contentsChecksumChan
			if val, ok := checksumsFound[bytesAndChecksum.fileChecksum]; !ok {
				checksumsFound[bytesAndChecksum.fileChecksum] = 1
			} else {
				checksumsFound[bytesAndChecksum.fileChecksum] = val + 1
			}
		}

		// Make sure we only got one input checksum, or we're bailing
		//fmt.Printf("\t\tChecksums seen: %v\n", checksumsFound)
		if len(checksumsFound) != 1 {
			panic("Inconsistent checksums for file " + inputFileInfo.RelativePath)
		}
		//fmt.Println("\t\tAll source copies are identical; proceeding with copy logic")

		canonicalFileBytes := bytesAndChecksum.fileContents

		// Determine unique dest relative path
		uniqueRelativePath := createUniqueRelativePath(inputFileInfo)
		//fmt.Printf("\t\tUnique relative path found that works for all dest dirs: %s\n", uniqueRelativePath)

		writersWg := &sync.WaitGroup{}
		for _, destDir := range inputFileInfo.DestinationDirs {
			currDestfilePath := destDir + string(os.PathSeparator) + uniqueRelativePath
			writersWg.Add(1)
			go writeFileContents(canonicalFileBytes, currDestfilePath, writersWg)
		}

		// Wait for all destination file writes to finish
		writersWg.Wait()

		//fmt.Printf("\t\tSuccessfully wrote %s to all destination directories!\n", uniqueRelativePath)

		// Note that the work for this input file is complete and the function can terminate cleanly
		wg.Done()

		//fmt.Printf("\tWork complete on relative path %s\n",
		//	inputFileInfo.RelativePath)
	}
}

func createUniqueRelativePath(inputFileInfo FileCopierRawfileInfo) string {
	// Iterate i 0 to 1000
	// Create file name using base filename & i
	// if i is zero, leave it off, otherwise append _nnnn to base
	// conflict = false
	// Iterate over dest dirs
	// if conflict exists in current dest dir with attempted path
	//		conflict = true
	//		break
	// if conflict == false
	//		break
	// end loop

	// Will have fallen out with unique filename that works in all dest dirs

	//fmt.Println("\t\tStarting to find unique destination relative path")
	var testRelativePath string
	var uniqueRelativePath string
	foundUniqueRelativePath := false
	for i := range 10000 {
		if i == 0 {
			testRelativePath = inputFileInfo.BaseFilename + inputFileInfo.FileExtension
		} else {
			testRelativePath = fmt.Sprintf("%s_%04d%s", inputFileInfo.BaseFilename,
				i, inputFileInfo.FileExtension)
		}

		intermediateDateDirectories := fmt.Sprintf("%04d%s%s",
			inputFileInfo.Timestamp.Year(), string(os.PathSeparator),
			iso8601Datetime(inputFileInfo.Timestamp)[:10])

		foundConflict := false
		for _, currDestDir := range inputFileInfo.DestinationDirs {
			conflictTest := currDestDir + string(os.PathSeparator) +
				intermediateDateDirectories + string(os.PathSeparator) +
				testRelativePath
			//fmt.Printf("\t\t\tChecking if %s exists\n", conflictTest)

			if _, err := os.Stat(conflictTest); err == nil {
				foundConflict = true
				break
			}
		}

		// If we hit an existing file, increment index
		if foundConflict == true {
			continue
		} else {
			uniqueRelativePath = intermediateDateDirectories + string(os.PathSeparator) +
				testRelativePath
			foundUniqueRelativePath = true
			break
		}
	}

	if !foundUniqueRelativePath {
		panic("Could not find relative path")
	}

	return uniqueRelativePath
}

func enumerateSourceDirs(programOpts ProgramOptions, timer *PerfTimer) map[string][]RawfileInfo {
	defer timer.exitFunction(timer.enterFunction("Enumerate images in source directories"))

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

	return foundFiles
}

func doCopyOperations(programOpts ProgramOptions, foundFiles map[string][]RawfileInfo, timer *PerfTimer) {
	defer timer.exitFunction(timer.enterFunction("Perform file copies, validate checksums of new files"))

	fmt.Println("\nStarting file checksumming/copying operations")

	fmt.Println("\tDestination directories:")
	for _, destDir := range programOpts.DestinationLocations {
		fmt.Printf("\t\t- %s\n", destDir)
	}

	workerChan := make(chan FileCopierRawfileInfo)

	// Fire off worker pool
	numWorkers := runtime.NumCPU()
	wg := &sync.WaitGroup{}
	for range numWorkers {
		go imageFileCopyWorker(workerChan, wg)
	}

	// Fire off work to the worker pool
	for currFileIndex := range len(foundFiles[programOpts.SourceDirs[0]]) {
		currFileToPopulate := foundFiles[programOpts.SourceDirs[0]][currFileIndex]

		inputFileAbsolutePaths := make([]string, len(programOpts.SourceDirs))
		for i, currSourceDir := range programOpts.SourceDirs {
			inputFileAbsolutePaths[i] = foundFiles[currSourceDir][currFileIndex].Paths.AbsolutePath
		}

		inputFileInfo := FileCopierRawfileInfo{
			currFileToPopulate.Paths.RelativePath,
			currFileToPopulate.BaseFilename,
			currFileToPopulate.FileExtension,
			currFileToPopulate.Timestamp,
			inputFileAbsolutePaths,
			programOpts.DestinationLocations}

		wg.Add(1)
		workerChan <- inputFileInfo

		//break
	}

	wg.Wait()

	fmt.Println("\n\tAll file copies with verified contents created!")
}

func printProfilingStats(programOpts ProgramOptions, functionTimer *PerfTimer, bytesInImageSet int64) {
	fmt.Println("\nPerformance Stats:")

	timerInfo := functionTimer.PerformanceStats()

	oneGB := float64(1024 * 1024 * 1024)

	sourceCopies := len(programOpts.SourceDirs)
	destCopies := len(programOpts.DestinationLocations)
	fileSetGB := float64(bytesInImageSet) / oneGB
	sourceGBRead := float64((int64(sourceCopies) * bytesInImageSet)) / oneGB
	destGBWritten := float64((int64(destCopies) * bytesInImageSet)) / oneGB
	destGBReadToVerify := destGBWritten
	totalGB := sourceGBRead + destGBWritten + destGBReadToVerify
	totalSeconds := timerInfo.cumulativeTime.Seconds()

	fmt.Println("\n\t          I/O Operation           File Set (GB)    Copies Of File Set      GB")
	fmt.Println("\t-------------------------------   -------------    ------------------   --------")
	fmt.Printf("\t             Source images read         %7.01f                    %2d    %7.01f\n", fileSetGB, sourceCopies, sourceGBRead)
	fmt.Printf("\t            Dest copies created         %7.01f                    %2d    %7.01f\n", fileSetGB, destCopies, destGBWritten)
	fmt.Printf("\tDest copies read back to verify         %7.01f                    %2d    %7.01f\n", fileSetGB, destCopies, destGBReadToVerify)
	fmt.Printf("\n\t                                          Total                    %2d    %7.01f   (%.01f GB/s average)\n",
		sourceCopies+(destCopies*2), totalGB, totalGB/totalSeconds)

	fmt.Println("\n\t                     Operation                         Time (s)    % of Total Time")
	fmt.Println("\t----------------------------------------------------   --------   ---------------")
	for _, currOpTime := range timerInfo.operationTimes {
		currSec := currOpTime.duration.Seconds()
		percentageTime := currSec / timerInfo.cumulativeTime.Seconds() * 100.0
		fmt.Printf("\t%52s   %8.01f            %5.01f%%\n",
			currOpTime.operationDescription,
			currOpTime.duration.Seconds(),
			percentageTime)
	}
	fmt.Printf("\n\t                                               Total   %8.01f   (%.02f minutes)\n", totalSeconds, totalSeconds/60)
}

func writeExiftoolGpsTagsIntoXmpInfo(geopoint gpx.Point, xmpInfo exiftool.FileMetadata) {
	//		https://exiftool.org/TagNames/GPS.html
	//
	// When adding GPS information to an image, it is important to set all of the following tags:
	//		- GPSLatitude
	//		- GPSLatitudeRef
	//		- GPSLongitude
	//		- GPSLongitudeRef
	//		- GPSAltitude & GPSAltitudeRef if the altitude is known.

	//fmt.Printf("\t\tLatitude %.05f, longitude %.05f, altitude %.01f m\n",
	//	geopoint.Latitude, geopoint.Longitude, geopoint.Elevation.Value())

	// GPSLatitude & GPSLatitudeRef
	xmpInfo.SetFloat("GPSLatitude", geopoint.Latitude)
	var gpsLatitudeRefValue string
	if geopoint.Latitude >= 0 {
		gpsLatitudeRefValue = "N"
	} else {
		gpsLatitudeRefValue = "S"
	}
	xmpInfo.SetString("GPSLatitudeRef", gpsLatitudeRefValue)

	// GPSLongitude & GPSLongitudeRef
	xmpInfo.SetFloat("GPSLongitude", geopoint.Longitude)
	var gpsLongitudeRefValue string
	if geopoint.Longitude < 0 {
		gpsLongitudeRefValue = "W"
	} else {
		gpsLongitudeRefValue = "E"
	}
	xmpInfo.SetString("GPSLongitudeRef", gpsLongitudeRefValue)

	// GPSAltitude & GPSAltitudeRef (optional)
	if geopoint.Elevation.NotNull() {
		xmpInfo.SetFloat("GPSAltitude", geopoint.Elevation.Value())
		// GPSAltitudeRef is 0 for "above sea level
		gpsAltitudeRef := int64(0)
		xmpInfo.SetInt("GPSAltitudeRef", gpsAltitudeRef)
	}
}

func geotagXmpWriterWorker(geotagWriteChannel chan RawfileInfo, wg *sync.WaitGroup) {
	// Sadly we can't run arbitrary exiftool commands through our exiftool wrapper like you can
	//		in the python version

	et, err := exiftool.NewExiftool()
	if err != nil {
		fmt.Printf("Error when initializing ExifTool: %v\n", err)
		return
	}

	// Range on a channel will repeatedly read until channel is empty AND channel is closed by sender
	for geotaggedSourceFile := range geotagWriteChannel {
		//fmt.Printf("\tCreating XMP with geotag for rawfile %s\n", geotaggedSourceFile.Paths.AbsolutePath)

		xmpFilename := geotaggedSourceFile.absoluteXmpFileWithGeotag

		// Create non-geotagged XMP .
		//fmt.Printf("\t\tXMP filename %s\n", xmpFilename)
		exiftoolArgs := []string{
			geotaggedSourceFile.Paths.AbsolutePath,
			"-o",
			xmpFilename,
		}

		cmd := exec.Command("exiftool", exiftoolArgs...)
		if err := cmd.Run(); err != nil {
			fmt.Printf("\t\t\tError running exiftool: %v\n", err)
		}

		// Read the generated XMP metadata in
		xmpInfoArray := et.ExtractMetadata(xmpFilename)

		//for _, currInfo := range xmpInfoArray {
		//	if currInfo.Err != nil {
		//		fmt.Printf("Got an error from exiftool metadata: %v\n", currInfo.Err)
		//	}
		//
		//	fmt.Printf("\t\tnew file info section\n")
		//
		//	for k, v := range currInfo.Fields {
		//		fmt.Printf("\t\t\t%v = %v\n", k, v)
		//	}
		//}

		// Insert geo tag metadata
		writeExiftoolGpsTagsIntoXmpInfo(geotaggedSourceFile.GeotaggedLocation, xmpInfoArray[0])

		// Write newly-geotagged metadata to the XMP file
		et.WriteMetadata(xmpInfoArray)

		// can mark one channel read complete
		wg.Done()
	}

	if err := et.Close(); err != nil {
		fmt.Printf("Error when closing exiftool: %v\n", err)
	}
}

func geotagSourceImages(sourceFiles []RawfileInfo, functionTimer *PerfTimer, gpxFile *os.File) {
	defer functionTimer.exitFunction(functionTimer.enterFunction("Geotag source images using GPX file"))
	fmt.Println("\nCreating geotagged XMP sidecar files using provided GPX file")

	fileInfo, err := gpxFile.Stat()

	if err != nil {
		panic("Could not get file stats for GPX file")
	}

	numberOfGpxBytes := fileInfo.Size()
	gpxBytes := make([]byte, numberOfGpxBytes)
	_, err = gpxFile.Read(gpxBytes)
	if err != nil {
		panic("Could not read GPX file")
	}

	parsedGpxfile, err := gpx.ParseBytes(gpxBytes)

	if err != nil {
		panic("Parsing GPX file contents failed")
	}

	//fmt.Println(parsedGpxfile.GetGpxInfo())

	successfulGeotags := 0

	// Use workers that launch Exiftool and write geotags into rawfiles to make Lightroom import cleaner
	//		Buffered channel to ensure writer never blocks
	geotagWriteChannel := make(chan RawfileInfo, successfulGeotags)

	numWorkers := runtime.NumCPU()
	//numWorkers := 1
	wg := &sync.WaitGroup{}
	for range numWorkers {
		go geotagXmpWriterWorker(geotagWriteChannel, wg)
	}

	// Iterate through all the files and geotag them -- could be parallelized, but no need,
	//		it's crazy fast even in a single thread
	for _, sourceFile := range sourceFiles {
		trackEntries := parsedGpxfile.PositionAt(sourceFile.Timestamp)
		if len(trackEntries) > 1 {
			fmt.Printf("WARN: somehow got multiple geotag position results for image %s, bailing on geotag",
				sourceFile.Paths.RelativePath)
			continue
		}

		if len(trackEntries) == 0 {
			fmt.Printf("\tINFO: could not geotag image %s (image time not found in GPX tracks)\n",
				sourceFile.Paths.RelativePath)
			continue
		}

		successfulGeotags++

		// Got a single point, which is really what we want
		trackEntry := trackEntries[0]
		gpsPoint := trackEntry.Point

		// add to the waitgroup BEFORE we issue work to ensure that Done won't be run before Add, thus causing
		//		the semaphore to go negative
		wg.Add(1)

		// Record geotag into the source file info
		sourceFile.GeotaggedLocation = gpsPoint

		// Write the XMP file that will contain the geotag
		inputDir := filepath.Dir(sourceFile.Paths.AbsolutePath)
		xmpFilename := inputDir + string(os.PathSeparator) + sourceFile.BaseFilename +
			".xmp"
		sourceFile.absoluteXmpFileWithGeotag = xmpFilename

		// Write sourcefile that we geotagged
		geotagWriteChannel <- sourceFile
	}

	// Close the channel to signal to workers all writes are done
	close(geotagWriteChannel)

	// Wait for workers to cleanly terminate
	wg.Wait()

	fmt.Printf("\tGeotagging complete; created geotagged XMP sidecars for %d of %d source files\n",
		successfulGeotags, len(sourceFiles))
}

func main() {
	programOpts := parseArgs()

	functionTimer := NewPerfTimer()

	foundFiles := enumerateSourceDirs(programOpts, functionTimer)

	// Make sure all file lists are identical
	if confirmIdenticalFilelists(foundFiles, functionTimer) == false {
		panic("File lists are not identical")
	}

	sizeOfOneCopyOfAllImages := getSizeOfOneCopyInBytes(foundFiles[programOpts.SourceDirs[0]],
		functionTimer)

	//		NOTE: we're using the fact that the array is passed by reference, because the target
	//			function updates fields in each element of the array we pass down into this function
	getRawfileDateTime(foundFiles[programOpts.SourceDirs[0]], functionTimer)

	if programOpts.GpxFile != nil {
		geotagSourceImages(foundFiles[programOpts.SourceDirs[0]], functionTimer, programOpts.GpxFile)
	}

	doCopyOperations(programOpts, foundFiles, functionTimer)

	printProfilingStats(programOpts, functionTimer, sizeOfOneCopyOfAllImages)
}
