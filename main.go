package main

import (
	"encoding/hex"
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/barasher/go-exiftool"
	"golang.org/x/crypto/sha3"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var p = message.NewPrinter(language.English)

type ProgramOptions struct {
	DebugMode            bool     `json:"debug_mode"`
	UtcOffsetHours       int      `json:"utc_offset_hours"`
	ChecksumThreads      int      `json:"checksum_processes"`
	FilenameExtension    string   `json:"filename_extension"`
	QueueLength          int      `json:"queue_length"`
	SourceDirs           []string `json:"source_dirs"`
	DestinationLocations []string `json:"destination_dirs"`
}

type PathInfo struct {
	AbsolutePath string
	RelativePath string
}

type RawfileInfo struct {
	Paths                   PathInfo
	BaseFilename            string
	FilesizeBytes           int
	Timestamp               time.Time
	OutputRelativeDirectory string
	OutputRelativePath      string
}

type timestampForRelativePath struct {
	relativePath      string
	computedTimestamp time.Time
}

type computedChecksum struct {
	absolutePath     string
	relativePath     string
	computedChecksum []byte
}

type fileContents struct {
	absolutePath string
	relativePath string
	fileBytes    []byte
}

type SourceManifests map[string][]RawfileInfo

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

	queueLength := parser.Int("", "queue_length", &argparse.Options{
		Required: false,
		Help:     "Length of channel to send checksums",
		Default:  9500,
	})

	defaultChecksumThreads := 4
	checksumThreads := parser.Int("", "checksum_processes", &argparse.Options{
		Required: false,
		Help:     "Number of checksum processes",
		Default:  defaultChecksumThreads,
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
		ChecksumThreads:      *checksumThreads,
		FilenameExtension:    *filenameExtension,
		QueueLength:          *queueLength,
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

func scanSourceDirForImages(sourceDir string, programOpts ProgramOptions) []RawfileInfo {
	var foundFiles []RawfileInfo

	//fmt.Printf("Scanning for sourcefiles in %s", sourceDir)

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

	//jsonBytes, _ := json.MarshalIndent(foundFiles, "", "    ")

	//fmt.Printf("Files in %s: \n%s\n", sourceDir, string(jsonBytes))

	return foundFiles
}

func createReverseMap(fileManifests SourceManifests) map[string][]*RawfileInfo {
	relativeToAbsoluteMap := make(map[string][]*RawfileInfo)

	// Create a map keyed by relative path, value is array of all the absolute paths that
	//		share that relative path
	for sourceDir, rawfilesInDir := range fileManifests {
		//fmt.Println("Iterating over rawfiles in sourcedir ", sourceDir)
		for index, currRawfile := range rawfilesInDir {
			currRelativePath := currRawfile.Paths.RelativePath

			currRawfileAddress := &(fileManifests[sourceDir][index])
			if currValue, ok := relativeToAbsoluteMap[currRelativePath]; !ok {
				// Initialize the array of rawfile pointers with the current rawfile pointer
				relativeToAbsoluteMap[currRelativePath] = []*RawfileInfo{currRawfileAddress}
			} else {
				// Append this pointer to the list of pointers
				relativeToAbsoluteMap[currRelativePath] = append(currValue, currRawfileAddress)
			}
		}
	}

	return relativeToAbsoluteMap

}

func timestampWorker(_ string, timestampRequestChannel chan RawfileInfo,
	timestampComputedChannel chan timestampForRelativePath, wg *sync.WaitGroup) {

	//fmt.Printf("Worker %s starting\n", workerName)

	et, err := exiftool.NewExiftool()
	if err != nil {
		panic(err)
	}

	datetimeFormatString := "2006-01-02 15:04:05"

	for {
		currEntryToTimestamp, ok := <-timestampRequestChannel

		// If our channel got closed by the parent, we're good to bail
		if !ok {
			break
		}

		currAbsPath := currEntryToTimestamp.Paths.AbsolutePath
		//fmt.Printf("Worker %s got file %s\n", workerName, currAbsPath)

		fileInfos := et.ExtractMetadata(currAbsPath)

		for _, fileInfo := range fileInfos {
			if fileInfo.Err != nil {
				//fmt.Printf("Error in fileinfo")
				continue
			}

			dateTimeOriginal, ok := fileInfo.Fields["DateTimeOriginal"]

			if !ok {
				panic("Could not find DateTimeOriginal field in file")
			}

			// Type assertion to get the value into a format we can handle
			s, ok := dateTimeOriginal.(string)

			if !ok {
				//fmt.Printf("Could not force %v into a string", v)
				continue
			}

			// Create valid datestring
			validDatetime := fmt.Sprintf("%04s-%02s-%02s %s",
				s[0:4], s[5:7], s[8:10], s[11:])

			myDatetime, err := time.Parse(datetimeFormatString, validDatetime)
			if err != nil {
				//fmt.Printf("error %s\n", err.Error())
				continue
			}
			timestampComputedChannel <- timestampForRelativePath{
				relativePath:      currEntryToTimestamp.Paths.RelativePath,
				computedTimestamp: myDatetime,
			}
		}
	}

	if err := et.Close(); err != nil {
		panic(err)
	}

	//fmt.Printf("Worker %s exiting cleanly\n", workerName)
	wg.Done()
}

func getExifTimestamps(fileManifests SourceManifests, programOpts ProgramOptions) {
	fmt.Println("\nRetrieving EXIF timestamps for all files")

	reverseMap := createReverseMap(fileManifests)

	//fmt.Println("\tDone creating reverse map")

	// Create two channels, one to send work to timestamp workers, one for workers to send timestamps back
	timestampRequestChannel := make(chan RawfileInfo, programOpts.QueueLength)
	timestampComputedChannel := make(chan timestampForRelativePath, programOpts.QueueLength)
	var wg sync.WaitGroup

	numTimestampWorkers := runtime.NumCPU() - 1
	for i := 0; i < numTimestampWorkers; i++ {
		workerName := fmt.Sprintf("worker_%02d", i+1)
		wg.Add(1)
		go timestampWorker(workerName, timestampRequestChannel, timestampComputedChannel, &wg)
		//fmt.Printf("launched %s\n", workerName)
	}

	//fmt.Println("Done launching workers")

	timestampsReceived := 0
	// First entry in the list of sourcedirs is the one we will use for timestamps
	timestampSourcedir := fileManifests[programOpts.SourceDirs[0]]
	timestampsExpected := len(timestampSourcedir)
	currSourceIndex := 0
	for timestampsReceived < timestampsExpected {
		if currSourceIndex < timestampsExpected {
			timestampRequestChannel <- timestampSourcedir[currSourceIndex]
			currSourceIndex++
			//fmt.Printf("Sent entry %d of %d in timestamp sourcedir\n",
			//	currSourceIndex, timestampsExpected)

			// Should we close the channel to signal we're done?
			if currSourceIndex == timestampsExpected {
				close(timestampRequestChannel)

				// Let our timestamp workers rejoin
				wg.Wait()
			}
		}

		// Select with default makes a non-blocking read
		timestampQueueExhausted := false
		for !timestampQueueExhausted {
			select {
			case computedTimestamp := <-timestampComputedChannel:
				timestampsReceived++

				//fmt.Printf("File %s got timestamp %s\n",
				//	computedTimestamp.relativePath, computedTimestamp.computedTimestamp.Format(time.RFC3339))

				manifestEntriesForThisRelativePath := reverseMap[computedTimestamp.relativePath]
				for _, currManifestEntry := range manifestEntriesForThisRelativePath {
					currManifestEntry.Timestamp = computedTimestamp.computedTimestamp
				}
				/*
					// if this carried us over a percentage mark, display progress
					oldPercentComplete := (float32(timestampsReceived-1) / float32(timestampsExpected)) * 100.0
					newPercentComplete := (float32(timestampsReceived) / float32(timestampsExpected)) * 100.0
					//fmt.Printf("New percent complete: %3.0f\n", newPercentComplete)
					if int(oldPercentComplete) != int(newPercentComplete) {
						fmt.Printf("Completed %d / %d (%3.0f%%) of timestamps\n",
							timestampsReceived, timestampsExpected, newPercentComplete)
					}

				*/
			default:
				// Nothing to read, break out of our loop
				//fmt.Println("Nothing to read from checksum channel, breaking out of loop")
				timestampQueueExhausted = true
			}
		}
	}

	if _, err := p.Printf("\tAll %d timestamps computed\n", timestampsExpected); err != nil {
		panic(err)
	}
}

func generateFileManifests(programOpts ProgramOptions) SourceManifests {
	fmt.Println("\nStarting to scan for all RAW files")
	sourceManifests := make(SourceManifests)

	totalFilesFound := 0

	for _, sourceDir := range programOpts.SourceDirs {
		sourceManifests[sourceDir] = scanSourceDirForImages(sourceDir, programOpts)

		totalFilesFound += len(sourceManifests[sourceDir])
	}

	if _, err := p.Printf("\tFound %d \".%s\" files in all sourcedirs\n", totalFilesFound,
		strings.ToUpper(programOpts.FilenameExtension)); err != nil {

		panic(err)
	}
	return sourceManifests
}

func splitExt(filename string) (base string, extension string) {
	filenameExtension := filepath.Ext(filename)
	filenameBase := filename[:len(filename)-len(filenameExtension)]

	return filenameBase, filenameExtension
}

func setDestinationFilenames(programOpts ProgramOptions, fileManifests SourceManifests) {
	fmt.Println("\nDetermining unique filenames in destination storage directories")

	// Resolve filename conflicts in the YYYY/YYYY-MM-DD destination dir
	conflictsFound := 0

	timestampsSourcedir := programOpts.SourceDirs[0]
	rawfileEntries := fileManifests[timestampsSourcedir]
	destDirPrefix := programOpts.DestinationLocations[0]

	for rawfileEntryIndex, currRawfileEntry := range rawfileEntries {
		currTimestamp := currRawfileEntry.Timestamp
		yearString := fmt.Sprintf("%4d", currTimestamp.Year())
		yearMonthDayString := fmt.Sprintf("%4d-%02d-%02d", currTimestamp.Year(),
			currTimestamp.Month(), currTimestamp.Day())
		//fmt.Printf("Got Year = %s, YMD = %s\n", yearString, yearMonthDayString)

		relativeOutputDirectory := filepath.Join(yearString, yearMonthDayString)

		rawfileEntries[rawfileEntryIndex].OutputRelativeDirectory = relativeOutputDirectory
		//fmt.Printf("Relative directory for %s: %s\n",
		//	currRawfileEntry.Paths.RelativePath, relativeOutputDirectory)

		candidateDestination := filepath.Join(destDirPrefix, yearString, yearMonthDayString,
			currRawfileEntry.BaseFilename)

		_, err := os.Stat(candidateDestination)

		// Break up filename into base and extension, tag on unique extension
		filenameBase, filenameExt := splitExt(currRawfileEntry.BaseFilename)
		uniqueExtension := 1
		for !os.IsNotExist(err) {
			conflictsFound++

			// Try a unique extension to the base that may not conflict
			candidateDestination = filepath.Join(destDirPrefix, yearString, yearMonthDayString,
				fmt.Sprintf("%s_%04d%s", filenameBase, uniqueExtension, filenameExt))

			uniqueExtension++
			//fmt.Printf("\tTesting updated version to see if there is no conflict: %s\n", candidateDestination)
			_, err = os.Stat(candidateDestination)
		}
		//fmt.Printf("\tFinal non-conflicting destination: %s\n", candidateDestination)

		rawfileEntries[rawfileEntryIndex].OutputRelativePath = filepath.Join(relativeOutputDirectory,
			filepath.Base(candidateDestination))

		//fmt.Printf("\tOutput relative path %s\n", rawfileEntries[rawfileEntryIndex].OutputRelativePath)
	}

	if _, err := p.Printf("\t%6d \".%s\" file(s) have had their unique destination paths determined\n",
		len(rawfileEntries), strings.ToUpper(programOpts.FilenameExtension)); err != nil {

		panic(err)
	}
	if _, err := p.Printf("\t%6d \".%s\" file(s) had their destination paths updated due to conflicts with existing files\n",
		conflictsFound, strings.ToUpper(programOpts.FilenameExtension)); err != nil {

		panic(err)
	}
}

func checksumWorker(checksumRequestChannel chan fileContents,
	checksumsComputedChannel chan computedChecksum, checksumWorkerWaitGroup *sync.WaitGroup) {

	for {
		currEntryToChecksum, ok := <-checksumRequestChannel

		// If our channel got closed by the parent, we're good to bail
		if !ok {
			break
		}

		shakeHash := make([]byte, 64)
		sha3.ShakeSum256(shakeHash, currEntryToChecksum.fileBytes)

		//fmt.Printf("\tComputed hash %x for file %s\n", string(shakeHash), currEntryToChecksum.absolutePath)

		checksumInfo := computedChecksum{
			absolutePath:     currEntryToChecksum.absolutePath,
			relativePath:     currEntryToChecksum.relativePath,
			computedChecksum: shakeHash,
		}

		checksumsComputedChannel <- checksumInfo
	}

	checksumWorkerWaitGroup.Done()
}

func launchChecksumWorkers(programOpts ProgramOptions,
	checksumsComputedChannel chan computedChecksum) (chan fileContents, *sync.WaitGroup) {

	// Create channel to send requests for checksums
	checksumRequestChannel := make(chan fileContents)

	checksumWorkerWaitGroup := sync.WaitGroup{}

	for i := 0; i < programOpts.ChecksumThreads; i++ {
		checksumWorkerWaitGroup.Add(1)
		go checksumWorker(checksumRequestChannel, checksumsComputedChannel, &checksumWorkerWaitGroup)
	}

	return checksumRequestChannel, &checksumWorkerWaitGroup
}

func destinationWriterWorker(programOpts ProgramOptions, destinationLocation string,
	writerIncomingWork chan fileContents, wg *sync.WaitGroup, checksumRequestChannel chan fileContents) {

	fmt.Printf("\tStarting destination writer for %s\n", destinationLocation)

	for {
		currEntryToWrite, ok := <-writerIncomingWork

		// If our channel got closed by the parent, we're good to bail
		if !ok {
			break
		}

		//fmt.Printf("\tRelative path for this file: %s\n", currEntryToWrite.relativePath)

		// See if we need to create any intermediate directories
		targetDir := filepath.Dir(filepath.Join(destinationLocation, currEntryToWrite.relativePath))
		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			fmt.Printf("\tDirectory %s did not exist, creating it", targetDir)
			if err := os.MkdirAll(targetDir, 0700); err != nil {
				panic(err)
			}
		}

		destAbsolutePath := filepath.Join(destinationLocation, currEntryToWrite.relativePath)

		// Write the contents to disk, read them back, and then ask for checksum
		if err := os.WriteFile(destAbsolutePath, currEntryToWrite.fileBytes, 0600); err != nil {
			panic(err)
		}

		// Read them bytes back
		readbackBytes, err := os.ReadFile(destAbsolutePath)
		if err != nil {
			panic(err)
		}

		// Checksum the bytes
		checksumData := fileContents{
			absolutePath: destAbsolutePath,
			relativePath: currEntryToWrite.relativePath,
			fileBytes:    readbackBytes,
		}
		checksumRequestChannel <- checksumData
	}

	wg.Done()
}

func sourceReaderWorker(programOpts ProgramOptions, sourceDirectory string, fileManifests SourceManifests,
	wg *sync.WaitGroup, checksumRequestChannel chan fileContents, writerChannels []chan fileContents) {

	fmt.Printf("\tStarting source reader for dir %s\n", sourceDirectory)

	// Read all files in our sourcedir
	for _, currRawfile := range fileManifests[sourceDirectory] {
		//fmt.Printf("Trying to read rawfile %s\n", currRawfile.Paths.AbsolutePath)

		bytesRead, err := os.ReadFile(currRawfile.Paths.AbsolutePath)

		if err != nil {
			panic(err)
		}

		fileInfo := fileContents{
			relativePath: currRawfile.OutputRelativePath,
			fileBytes:    bytesRead,
		}

		// Request checksum of file contents
		checksumRequestChannel <- fileInfo

		// Send to all destination writers
		for _, currFileWriterChannel := range writerChannels {
			currFileWriterChannel <- fileInfo
		}
	}

	//fmt.Printf("\tDone with sourcedir reader for %s\n", sourceDirectory)
	wg.Done()
}

func launchFileReadersWriters(programOpts ProgramOptions, fileManifests SourceManifests,
	checksumRequestChannel chan fileContents) (*sync.WaitGroup, []chan fileContents, *sync.WaitGroup) {

	// Keep list of channels to send file contents to destination writers
	writerChannels := make([]chan fileContents, len(programOpts.DestinationLocations))

	destinationWritersWaitGroup := sync.WaitGroup{}
	// Launch Writers
	for destIndex, destinationLocation := range programOpts.DestinationLocations {
		destinationWritersWaitGroup.Add(1)

		// Make the channel for this destination writer
		writerChannels[destIndex] = make(chan fileContents)
		go destinationWriterWorker(programOpts, destinationLocation, writerChannels[destIndex],
			&destinationWritersWaitGroup, checksumRequestChannel)
	}

	// Launch Readers
	sourceReadersWaitGroup := sync.WaitGroup{}
	for _, sourceDirectory := range programOpts.SourceDirs {
		sourceReadersWaitGroup.Add(1)
		go sourceReaderWorker(programOpts, sourceDirectory, fileManifests, &sourceReadersWaitGroup,
			checksumRequestChannel, writerChannels)
	}

	return &sourceReadersWaitGroup, writerChannels, &destinationWritersWaitGroup
}

func main() {
	programOpts := parseArgs()
	fileManifests := generateFileManifests(programOpts)

	// TODO: Make sure source file manifests match

	getExifTimestamps(fileManifests, programOpts)

	// Establish unique destination filenames
	setDestinationFilenames(programOpts, fileManifests)

	fmt.Printf("\nStarting copy/checksum operations\n")

	// Launch checksum workers
	checksumsComputedChannel := make(chan computedChecksum)
	checksumRequestChannel, checksumWorkerWaitGroup := launchChecksumWorkers(programOpts,
		checksumsComputedChannel)

	// Launch readers/writers
	sourceReaderWaitGroup, writerChannels, destWriterWaitGroup := launchFileReadersWriters(programOpts,
		fileManifests, checksumRequestChannel)

	// Read checksums out
	numChecksums := 1871 * 2 // TODO: compute this
	receivedChecksums := make(map[string]map[string]int)
	for i := 0; i < numChecksums; i++ {
		checksumInfo := <-checksumsComputedChannel

		hexChecksum := hex.EncodeToString(checksumInfo.computedChecksum)

		// Is the relative path in our map yet?
		if subMap, ok := receivedChecksums[checksumInfo.relativePath]; !ok {
			receivedChecksums[checksumInfo.relativePath] = make(map[string]int)

			// Add new entry
			receivedChecksums[checksumInfo.relativePath][hexChecksum] = 1
		} else {
			// If we've seen this checksum before, increment its count
			if checksumCount, ok := subMap[hexChecksum]; !ok {
				// First time we've seen this checksum
				subMap[hexChecksum] = 1
			} else {
				subMap[hexChecksum] = checksumCount + 1
			}
		}
	}

	p.Printf("\tAll %d checksums received\n", numChecksums)

	// Signal all readers and writers can come home
	for _, currWriterChannel := range writerChannels {
		close(currWriterChannel)
	}
	close(checksumRequestChannel)
	close(checksumsComputedChannel)

	// Land all the readers and writers
	sourceReaderWaitGroup.Wait()
	destWriterWaitGroup.Wait()

	// We can now close the channel for requests to signal the checksum writers can come home

	// Land all the checksum workers now that the readers and writers are done
	checksumWorkerWaitGroup.Wait()

	// Now that we've done all checksums, make sure they all MATCH

	// Print IO stats

	// Print performance stats
}
