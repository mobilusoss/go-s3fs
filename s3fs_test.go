package s3fs

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var fs *S3FS

func setup() {
	fs = New(&Config{
		EnableMinioCompat: true,
		Endpoint: "http://127.0.0.1:9000",
		EnableIAMAuth: true,
		AccessKeyID: "accesskey",
		AccessSecretKey: "secretkey",
		Bucket: "test",
	})
}

func teardown() {
	if fs != nil {
		if err := fs.BulkDelete("/"); err != nil {
			println("teardown error:", err.Error())
		}
	}
}

func TestMain(m *testing.M) {
	setup()
	exitCode := m.Run()
	teardown()

	os.Exit(exitCode)
}

func TestS3FS_List_1(t *testing.T) {
	list := fs.List("/")
	if list == nil {
		t.Fatal("cannot connect s3")
	}
	if len(*list) != 0 {
		t.Fatal("invalid state")
	}
}

func TestS3FS_Put1(t *testing.T) {
	body := []byte("this is test string")
	readCloser := ioutil.NopCloser(bytes.NewReader(body))

	if err := fs.Put("testfile", readCloser, "text/plain"); err != nil {
		t.Fatal(err)
	}
}

func TestS3FS_List_2(t *testing.T) {
	list := fs.List("/")
	if list == nil {
		t.Fatal("cannot connect s3")
	}
	if len(*list) != 1 {
		t.Fatal("invalid state")
	}
	file := (*list)[0]
	if file.Name != "testfile" {
		t.Fatal("invalid file name:", file.Name)
	}
	if file.Path != "/testfile" {
		t.Fatal("invalid file path:", file.Path)
	}
	if file.Size != int64(len([]byte("this is test string"))) {
		t.Fatal("invalid file size:", file.Size)
	}
	if file.Type != File {
		t.Fatal("invalid file type:", file.Type)
	}
}

func TestS3FS_Get(t *testing.T) {
	t.Run("exists", func(st *testing.T){
		readCloser, err := fs.Get("/testfile")
		if err != nil {
			st.Fatal("get file error:", err)
		}
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(*readCloser); err != nil {
			st.Fatal("io error:", err)
		}
		body := buf.String()
		if body != "this is test string" {
			st.Fatal("invalid data")
		}
	})
	t.Run("non exists", func(st *testing.T){
		_, err := fs.Get("/foobar")
		if err == nil {
			st.Fatal("io error:", err)
		}
	})
}

func TestS3FS_MkDir(t *testing.T) {
	if err := fs.MkDir("/testdir1"); err != nil {
		t.Fatal(err)
	}

	list := fs.List("/testdir1")
	if list == nil {
		t.Fatal("cannot connect s3")
	}
	if len(*list) != 1 {
		t.Fatal("invalid state")
	}
	file := (*list)[0]
	if file.Name != "testdir1" {
		t.Fatal("invalid file name:", file.Name)
	}
	if file.Path != "/testdir1/" {
		t.Fatal("invalid file path:", file.Path)
	}
	if file.Type != Directory {
		t.Fatal("invalid file type:", file.Type)
	}
}

func TestS3FS_MkDirP(t *testing.T) {
	if err := fs.MkDir("/testdir2/child"); err != nil {
		t.Fatal(err)
	}

	list := fs.List("/testdir2/")
	if list == nil {
		t.Fatal("cannot connect s3")
	}
	if len(*list) != 1 {
		t.Fatal("invalid state")
	}
	file := (*list)[0]
	if file.Name != "child" {
		t.Fatal("invalid file name:", file.Name)
	}
	if file.Path != "/testdir2/child/" {
		t.Fatal("invalid file path:", file.Path)
	}
	if file.Type != Directory {
		t.Fatal("invalid file type:", file.Type)
	}
}

func TestS3FS_Copy(t *testing.T) {
	if err := fs.Copy("/testfile", "/testdir1/testfile", nil); err != nil {
		t.Fatal("copy error:", err)
	}

	readCloser, err := fs.Get("/testdir1/testfile")
	if err != nil {
		t.Fatal("get file error:", err)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(*readCloser); err != nil {
		t.Fatal("io error:", err)
	}
	body := buf.String()
	if body != "this is test string" {
		t.Fatal("invalid data")
	}
}

func TestS3FS_Move(t *testing.T) {
	beforeList := fs.List("/testdir1")
	if beforeList == nil {
		t.Fatal("cannot connect s3")
	}

	if err := fs.Move("/testdir1/", "/testdir2/"); err != nil {
		t.Fatal("move error:", err)
	}

	afterList := fs.List("/testdir2/testdir1")
	if afterList == nil {
		t.Fatal("cannot connect s3")
	}

	if len(*beforeList) != len(*afterList) {
		t.Fatal("invalid files:", *beforeList, *afterList)
	}
	for i := range *beforeList {
		if (*beforeList)[i].Name != (*afterList)[i].Name {
			t.Fatal("name error:", (*beforeList)[i].Name, (*afterList)[i].Name)
		}
		if (*beforeList)[i].Size != (*afterList)[i].Size {
			t.Fatal("size error:", (*beforeList)[i].Size, (*afterList)[i].Size)
		}
		if (*beforeList)[i].Type != (*afterList)[i].Type {
			t.Fatal("type error:", (*beforeList)[i].Type, (*afterList)[i].Type)
		}
	}
}

func TestS3FS_Delete(t *testing.T) {
	if err := fs.Delete("/testfile"); err != nil {
		t.Fatal("copy error:", err)
	}

	_, err := fs.Get("/testfile")
	if err == nil {
		t.Fatal("io error:", err)
	}
}