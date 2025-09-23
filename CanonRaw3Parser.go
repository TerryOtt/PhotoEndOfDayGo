package main

import (
	"os"
	"time"

	"github.com/evanoberholster/imagemeta"
)

func GetCR3DatetimeUTC(cr3Filepath string) (time.Time, error) {
	//fmt.Println("Requested to get creation timestamp for", cr3Filepath)

	fileHandle, err := os.Open(cr3Filepath)
	if err != nil {
		return time.Time{}, err
	}
	defer fileHandle.Close()

	exifData, err := imagemeta.Decode(fileHandle)
	if err != nil {
		return time.Time{}, err
	}

	return exifData.DateTimeOriginal(), nil
}
