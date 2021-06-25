package roaring

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func RetrieveRealDataBitmaps(datasetName string) ([][]uint32, error) {
	datasetPath := datasetName + ".zip"

	if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("dataset %s does not exist, tried path: %s", datasetName, datasetPath)
	}

	zipFile, err := zip.OpenReader(datasetPath)
	if err != nil {
		return nil, fmt.Errorf("error opening dataset %s zipfile, cause: %v", datasetPath, err)
	}
	defer zipFile.Close()

	var largestFileSize uint64
	for _, f := range zipFile.File {
		if f.UncompressedSize64 > largestFileSize {
			largestFileSize = f.UncompressedSize64
		}
	}

	bitmaps := make([][]uint32, len(zipFile.File))
	buf := make([]byte, largestFileSize)
	var bufStep uint64 = 32768 // apparently the largest buffer zip can read
	for i, f := range zipFile.File {
		r, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to read bitmap file %s from dataset %s, cause: %v", f.Name, datasetName, err)
		}

		var totalReadBytes uint64

		for {
			var endOffset uint64
			if f.UncompressedSize64 < totalReadBytes+bufStep {
				endOffset = f.UncompressedSize64
			} else {
				endOffset = totalReadBytes + bufStep
			}

			readBytes, err := r.Read(buf[totalReadBytes:endOffset])
			totalReadBytes += uint64(readBytes)

			if err == io.EOF {
				r.Close()
				break
			} else if err != nil {
				r.Close()
				return nil, fmt.Errorf("could not read content of file %s from dataset %s, cause: %v", f.Name, datasetName, err)
			}
		}

		elemsAsBytes := bytes.Split(buf[:totalReadBytes], []byte{44}) // 44 is a comma

		b := make([]uint32, 0, 128)
		for _, elemBytes := range elemsAsBytes {
			elemStr := strings.TrimSpace(string(elemBytes))

			e, err := strconv.ParseUint(elemStr, 10, 32)
			if err != nil {
				r.Close()
				return nil, fmt.Errorf("could not parse %s as uint32. Reading %s from %s. Cause: %v", elemStr, f.Name, datasetName, err)
			}

			b = append(b, uint32(e))
		}
		bitmaps[i] = b
	}

	return bitmaps, nil
}
