package server

import (
	"bytes"
	"errors"
	"io"
)

type FlowDriver struct {
	FunCallBack func(name string, b []byte, num int64) error
}

func (driver *FlowDriver) Init(conn *Conn) {
}

func (driver *FlowDriver) Stat(path string) (FileInfo, error) {
	return nil, errors.New("Not Support")
}

func (driver *FlowDriver) ChangeDir(path string) error {
	return errors.New("Not Support")
}
func (driver *FlowDriver) ListDir(path string, callback func(FileInfo) error) error {
	return errors.New("Not Support")
}

func (driver *FlowDriver) DeleteDir(path string) error {
	return errors.New("Not Support")
}
func (driver *FlowDriver) DeleteFile(path string) error {
	return errors.New("Not Support")
}

func (driver *FlowDriver) Rename(fromPath string, toPath string) error {
	return errors.New("Not Support")
}

func (driver *FlowDriver) MakeDir(path string) error {
	return errors.New("Not Support")
}

func (driver *FlowDriver) GetFile(path string, offset int64) (int64, io.ReadCloser, error) {
	return 0, nil, errors.New("Not Support")
}

func (driver *FlowDriver) PutFile(destPath string, data io.Reader, appendData bool) (int64, error) {

	b := bytes.NewBuffer(nil)

	n, err := b.ReadFrom(data)
	if err != nil {
		return 0, nil
	}

	if driver.FunCallBack != nil {
		driver.FunCallBack(destPath, b.Bytes(), n)
	}

	return n, nil
}

type FlowDriverFactory struct {
	FunCallBack func(name string, b []byte, num int64) error
}

func (factory *FlowDriverFactory) NewDriver() (Driver, error) {
	return &FlowDriver{factory.FunCallBack}, nil
}
