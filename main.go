package main

import (
	"fmt"
	"github.com/akamensky/argparse"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func processSourceFile(sourcefileInfo RawfileInfo, programOpts ProgramOptions, wg *sync.WaitGroup) {
	// Get checksum for source file
	sourceAbsolutePath := sourcefileInfo.Paths.AbsolutePath
	//fmt.Printf("Goroutine starting up to process sourcefile %s", sourcefileInfo.Paths.AbsolutePath)

	// Write to all destination directories
	for _, currOutputDir := range programOpts.DestinationLocations {
		fmt.Printf("\tWriting source %s to dest %s\n", sourceAbsolutePath, currOutputDir)
	}

	// make sure all write checksums match the source checksum

	// We're done processing this file
	wg.Done()
}

func main() {
	programOpts := parseArgs()
	//_ = parseArgs()

	// Create channel and waitgroup for the enumeration goroutines to write their file lists back to main
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

	// Kick off a goroutine to process each sourcefile, alternating between sourcedirs to keep equal load on both
	numFilesPerSourcedir := len(foundFiles[programOpts.SourceDirs[0]])

	wg := &sync.WaitGroup{}

	for i := 0; i < numFilesPerSourcedir; i++ {
		for _, currSourcedir := range programOpts.SourceDirs {
			wg.Add(1)
			go processSourceFile(foundFiles[currSourcedir][i], programOpts, wg)
		}
	}

	wg.Wait()
	fmt.Println("All copies have been written")
}
