package filemanager

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/gwDistSys20/project-kafkaesque/config"
)

var syncOnce sync.Once
var instance *FileManager
var currentFolder string

// FileManager struct keeps track of all the files in the broker
type FileManager struct {
	folders []int
	files   []int
	// partitions partitions
}

// NewFileManager instance
func NewFileManager() *FileManager {
	syncOnce.Do(func() {
		c := config.GetConfig()
		brokerdir := c.BrokerDir
		//fmt.Println("Current address of broker",brokerdir)
		err := os.MkdirAll(brokerdir, 0777)
		if err != nil {
			log.Print(err)
			log.Fatal("Failed to create File Manager Directory for the Broker")
		}
		instance = &FileManager{folders: []int{}}

	})
	return instance
}

func GetFileManager() *FileManager {
	return instance
}

// number of partitions of a particular topic

// countLines function counts the lines in current file
func (fm *FileManager) CountLines(relPath string) int {
	c := config.GetConfig()
	fullpath := path.Join(c.BrokerDir, relPath)

	data, err := os.OpenFile(fullpath, os.O_RDONLY, 0777)
	if err != nil {
		log.Println("File reading error", err)
	}
	scanner := bufio.NewScanner(data)
	result := []string{}
	// Use Scan.
	for scanner.Scan() {
		//fmt.Println("Inside scanner")
		line := scanner.Text()
		// Append line to result.
		//fmt.Println("Line:",line)
		result = append(result, line)
	}
	return len(result)
}

// AddFile method to create/append to a log file
func (fm *FileManager) AddFile(relativePath string, wg *sync.WaitGroup) {
	defer wg.Done()
	c := config.GetConfig()
	patharr := strings.Split(relativePath, "/")
	basePath := strings.Join(patharr[:len(patharr)-1], "/")

	// if base path does not exist, create it
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		fm.AddFolder(basePath)
	}
	fullpath := path.Join(c.BrokerDir, relativePath)
	//fmt.Println("Full path",fullpath)
	f, err := os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_RDONLY, 0777)
	if err != nil {
		fmt.Println(err)
	}
	f.Close()
}

func (fm *FileManager) Append(fileName string, text []byte) (int, bool) {
	c := config.GetConfig()
	fullpath := path.Join(c.BrokerDir, fileName)
	f, err := os.OpenFile(fullpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	defer f.Close()
	if err != nil {
		log.Println(err)
		return 0, false
	}
	_, err = f.Write(text)
	if err != nil {
		log.Println(err)
	}
	f.WriteString("\n")
	lineNumber := fm.CountLines(fileName)
	return lineNumber, true
}

// AddFolder method to add folders to disk
func (fm *FileManager) AddFolder(relativePath string) error {
	c := config.GetConfig()
	path := path.Join(c.BrokerDir, relativePath)
	//fmt.Println("Path",path)
	err := os.MkdirAll(path, 0777)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

// FetchLogIndex takes in relative path and offset and returns the logIndex
func (fm *FileManager) FetchLogIndex(relPath string, offset string) (logIndex string, ok bool) {
	c := config.GetConfig()
	fullpath := path.Join(c.BrokerDir, relPath)
	f, err := os.OpenFile(fullpath, os.O_RDONLY, 0777)

	if err != nil {
		log.Println("File reading error", err)
	}
	scanner := bufio.NewScanner(f)

	// Scan each entry
	for scanner.Scan() {
		line := scanner.Text()
		entry := strings.Split(line, ",")
		currentOffset := entry[0]

		// if offset is found return
		if offset == currentOffset {
			logIndex = entry[1]
			return logIndex, true
		}
	}
	return "", false
}

// FetchMessages all messages after the logIndex provided
func (fm *FileManager) FetchMessages(relPath string, logIndex string, cb func([]byte)) {
	c := config.GetConfig()
	fullpath := path.Join(c.BrokerDir, relPath)
	f, err := os.OpenFile(fullpath, os.O_RDONLY, 0777)

	if err != nil {
		log.Println("File reading error", err)
	}
	index, _ := strconv.Atoi(logIndex)
	scanner := bufio.NewScanner(f)
	line := 1

	// Scan each entry
	for scanner.Scan() {
		if line >= index {
			binaryMessage := scanner.Bytes()
			cb(binaryMessage)
		}
		line++

	}
}
