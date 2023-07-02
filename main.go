package main

import (
	"fmt"
	"github.com/akamensky/argparse"
	"github.com/barasher/go-exiftool"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

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
	Paths         PathInfo
	FilesizeBytes int
	Timestamp     time.Time
}

type timestampForRelativePath struct {
	relativePath      string
	computedTimestamp time.Time
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

			newRawfile := RawfileInfo{
				Paths: PathInfo{
					path,
					strings.TrimPrefix(path, sourceDir+"\\"),
				},
				FilesizeBytes: int(info.Size()),
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

func createReverseMap(fileManifests map[string][]RawfileInfo) map[string][]*RawfileInfo {
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

func timestampWorker(workerName string, timestampRequestChannel chan RawfileInfo,
	timestampComputedChannel chan timestampForRelativePath) {

	//fmt.Printf("Worker %s starting\n", workerName)

	et, err := exiftool.NewExiftool()
	if err != nil {
		panic(err)
	}

	defer et.Close()

	datetimeFormatString := "2006-01-02 15:04:05"

	for {
		currEntryToTimestamp := <-timestampRequestChannel

		currAbsPath := currEntryToTimestamp.Paths.AbsolutePath
		//fmt.Printf("Worker %s got file %s\n", workerName, currAbsPath)

		fileInfos := et.ExtractMetadata(currAbsPath)

		for _, fileInfo := range fileInfos {
			if fileInfo.Err != nil {
				//fmt.Printf("Error in fileinfo")
				continue
			}

			for k, v := range fileInfo.Fields {
				if k == "DateTimeOriginal" {

					//fmt.Printf("%s: [%v] %v\n", workerName, k, v)

					// Type assertion to get the value into a format we can handle
					if s, ok := v.(string); !ok {
						//fmt.Printf("Could not force %v into a string", v)
						continue
					} else {
						// Create valid datestring
						validDatetime := fmt.Sprintf("%04s-%02s-%02s %s",
							s[0:4], s[5:7], s[8:10], s[11:])

						if myDatetime, err := time.Parse(datetimeFormatString, validDatetime); err != nil {
							//fmt.Printf("error %s\n", err.Error())
							continue
						} else {
							timestampComputedChannel <- timestampForRelativePath{
								relativePath:      currEntryToTimestamp.Paths.RelativePath,
								computedTimestamp: myDatetime,
							}
						}
					}
				}
			}
		}
	}
}

func getExifTimestamps(fileManifests map[string][]RawfileInfo, programOpts ProgramOptions) {
	fmt.Println("\nRetrieving EXIF timestamps for all files")

	reverseMap := createReverseMap(fileManifests)

	//fmt.Println("\tDone creating reverse map")

	// Create two channels, one to send work to timestamp workers, one for workers to send timestamps back
	timestampRequestChannel := make(chan RawfileInfo, programOpts.QueueLength)
	timestampComputedChannel := make(chan timestampForRelativePath, programOpts.QueueLength)

	numTimestampWorkers := runtime.NumCPU() - 1
	for i := 0; i < numTimestampWorkers; i++ {
		workerName := fmt.Sprintf("worker_%02d", i+1)
		go timestampWorker(workerName, timestampRequestChannel, timestampComputedChannel)
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

	fmt.Printf("\tAll %d timestamps computed", timestampsExpected)
}

func generateFileManifests(programOpts ProgramOptions) map[string][]RawfileInfo {
	fmt.Println("\nStarting to scan for all RAW files")
	sourceManifests := make(map[string][]RawfileInfo)

	totalFilesFound := 0

	for _, sourceDir := range programOpts.SourceDirs {
		sourceManifests[sourceDir] = scanSourceDirForImages(sourceDir, programOpts)

		totalFilesFound += len(sourceManifests[sourceDir])
	}

	fmt.Printf("\tFound %d RAW files in all sourcedirs\n", totalFilesFound)
	return sourceManifests
}

func main() {
	programOpts := parseArgs()
	fileManifests := generateFileManifests(programOpts)
	getExifTimestamps(fileManifests, programOpts)
}
