package issue20

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/mobilusoss/go-s3fs"
)

const bucket = "issue20"

var fs *s3fs.S3FS

func setup() {
	endpoint := "http://127.0.0.1:9000"
	if os.Getenv("DRONE") == "true" {
		endpoint = "http://minio:9000"
	}
	fs = s3fs.New(&s3fs.Config{
		EnableMinioCompat: true,
		Endpoint:          endpoint,
		EnableIAMAuth:     true,
		AccessKeyID:       "accesskey",
		AccessSecretKey:   "secretkey",
		Bucket:            bucket,
	})
}

func teardown() {
	if fs != nil {
		_ = fs.DeleteBucket(bucket)
	}
}

func TestMain(m *testing.M) {
	setup()
	exitCode := m.Run()
	teardown()

	os.Exit(exitCode)
}

func TestIssue20_CreateBucket(t *testing.T) {
	if err := fs.CreateBucket(bucket); err != nil {
		t.Fatal("bucket create error:", err)
	}
}

func TestIssue20_Put(t *testing.T) {
	body := []byte("this is test string")
	readCloser := ioutil.NopCloser(bytes.NewReader(body))

	if err := fs.Put("test%file", readCloser, "text/plain"); err != nil {
		t.Fatal(err)
	}
}

func TestIssue20_Info(t *testing.T) {
	t.Run("file", func(st *testing.T) {
		info := fs.Info("/test%file")
		if info == nil {
			st.Fatal("s3 error")
		}
		length := *info.ContentLength
		if length != int64(len([]byte("this is test string"))) {
			st.Fatal("io error")
		}
	})
}

func TestIssue20_Get(t *testing.T) {
	readCloser, err := fs.Get("/test%file")
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

func TestIssue20_List(t *testing.T) {
	list := fs.List("/")
	if list == nil {
		t.Fatal("cannot connect s3")
	}
	if len(*list) != 1 {
		t.Fatal("invalid state")
	}
	file := (*list)[0]
	if file.Name != "test%file" {
		t.Fatal("invalid file name:", file.Name)
	}
	if file.Path != "/test%file" {
		t.Fatal("invalid file path:", file.Path)
	}
	if file.Size != int64(len([]byte("this is test string"))) {
		t.Fatal("invalid file size:", file.Size)
	}
	if file.Type != s3fs.File {
		t.Fatal("invalid file type:", file.Type)
	}
}

func TestIssue20_MkDir(t *testing.T) {
	t.Run("mkdir", func(st *testing.T) {
		if err := fs.MkDir("/test%dir1"); err != nil {
			t.Fatal(err)
		}

		list := fs.List("/test%dir1")
		if list == nil {
			t.Fatal("cannot connect s3")
		}
		if len(*list) != 1 {
			t.Fatal("invalid state")
		}
		file := (*list)[0]
		if file.Name != "test%dir1" {
			t.Fatal("invalid file name:", file.Name)
		}
		if file.Path != "/test%dir1/" {
			t.Fatal("invalid file path:", file.Path)
		}
		if file.Type != s3fs.Directory {
			t.Fatal("invalid file type:", file.Type)
		}
	})
	t.Run("mkdir -p", func(st *testing.T) {
		if err := fs.MkDir("/test%dir2/child"); err != nil {
			t.Fatal(err)
		}

		list := fs.List("/test%dir2/")
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
		if file.Path != "/test%dir2/child/" {
			t.Fatal("invalid file path:", file.Path)
		}
		if file.Type != s3fs.Directory {
			t.Fatal("invalid file type:", file.Type)
		}
	})
}

func TestIssue20_Copy(t *testing.T) {
	t.Run("single copy", func(st *testing.T) {
		if err := fs.Copy("/test%file", "/test%dir1/test%file", nil); err != nil {
			st.Fatal("copy error:", err)
		}
		readCloser, err := fs.Get("/test%dir1/test%file")
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
	t.Run("single copy with metadata", func(st *testing.T) {
		metadataKey := "Test-Metadata"
		metadataValue := "test"
		if err := fs.Copy("/test%file", "/test%metadata", map[string]string{
			metadataKey: metadataValue,
		}); err != nil {
			st.Fatal("copy error:", err)
		}
		readCloser, err := fs.Get("/test%dir1/test%file")
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
		info := fs.Info("/test%metadata")
		if info == nil {
			st.Fatal("s3 info error")
		}
		s3Metadata := info.Metadata[metadataKey]
		if s3Metadata != metadataValue {
			st.Fatal("s3 metadata error:", s3Metadata)
		}
	})
	t.Run("bulk copy", func(st *testing.T) {
		if err := fs.MkDir("/bulkcopy_a"); err != nil {
			st.Fatal(err)
		}
		if err := fs.Copy("/test%file", "/bulkcopy_a/test%file", nil); err != nil {
			st.Fatal("copy error:", err)
		}
		if err := fs.MkDir("/bulkcopy_b"); err != nil {
			st.Fatal("mkdir error:", err)
		}
		if err := fs.Copy("/bulkcopy_a/", "/bulkcopy_b/", nil); err != nil {
			st.Fatal("copy error:", err)
		}

		readCloser, err := fs.Get("/bulkcopy_b/bulkcopy_a/test%file")
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
}

func TestIssue20_PathExists(t *testing.T) {
	t.Run("root exists", func(st *testing.T) {
		exists := fs.PathExists("/")
		if exists != true {
			t.Fatal("root path doesnt exist")
		}
	})
	t.Run("file exists", func(st *testing.T) {
		exists := fs.PathExists("/test%file")
		if exists != true {
			t.Fatal("root path doesnt exist")
		}
	})
	t.Run("folder exists", func(st *testing.T) {
		exists := fs.PathExists("/test%dir1")
		if exists != true {
			t.Fatal("root path doesnt exist")
		}
	})
	t.Run("non exists file", func(st *testing.T) {
		exists := fs.PathExists("/dummyfile")
		if exists == true {
			t.Fatal("dummyfile shouldnt exist")
		}
	})
	t.Run("non exists folder", func(st *testing.T) {
		exists := fs.PathExists("/dummydir/")
		if exists == true {
			t.Fatal("dummydir shouldnt exist")
		}
	})
}

func TestIssue20_ExactPathExists(t *testing.T) {
	t.Run("exact file exists", func(st *testing.T) {
		exists := fs.ExactPathExists("/test%file")
		if exists != true {
			t.Fatal("file doesn't exist")
		}
	})
	t.Run("exact file doesn't exists", func(st *testing.T) {
		exists := fs.ExactPathExists("/test%file2")
		if exists == true {
			t.Fatal("file exists")
		}
	})
}

func TestIssue20_Move(t *testing.T) {
	t.Run("single", func(st *testing.T) {
		if err := fs.MkDir("/singlemove"); err != nil {
			st.Fatal("mkdir error:", err)
		}
		if err := fs.Copy("/test%file", "/move%test", nil); err != nil {
			st.Fatal("cp error:", err)
		}
		if err := fs.Move("/move%test", "/singlemove/move%test"); err != nil {
			st.Fatal("mv error:", err)
		}
	})
	t.Run("bulk", func(st *testing.T) {
		beforeList := fs.List("/test&dir1")
		if beforeList == nil {
			st.Fatal("cannot connect s3")
		}

		if err := fs.Move("/test&dir1/", "/test&dir2/"); err != nil {
			st.Fatal("move error:", err)
		}

		afterList := fs.List("/test&dir2/test&dir1")
		if afterList == nil {
			st.Fatal("cannot connect s3")
		}

		if len(*beforeList) != len(*afterList) {
			st.Fatal("invalid files:", *beforeList, *afterList)
		}
		for i := range *beforeList {
			if (*beforeList)[i].Name != (*afterList)[i].Name {
				st.Fatal("name error:", (*beforeList)[i].Name, (*afterList)[i].Name)
			}
			if (*beforeList)[i].Size != (*afterList)[i].Size {
				st.Fatal("size error:", (*beforeList)[i].Size, (*afterList)[i].Size)
			}
			if (*beforeList)[i].Type != (*afterList)[i].Type {
				st.Fatal("type error:", (*beforeList)[i].Type, (*afterList)[i].Type)
			}
		}
	})
}

func TestIssue20_Delete(t *testing.T) {
	t.Run("rm", func(st *testing.T) {
		if err := fs.Delete("/test%file"); err != nil {
			t.Fatal("copy error:", err)
		}

		_, err := fs.Get("/test%file")
		if err == nil {
			t.Fatal("io error:", err)
		}
	})
	t.Run("rm -r", func(st *testing.T) {
		if err := fs.Delete("/"); err != nil {
			t.Fatal("copy error:", err)
		}
		list := fs.List("/")
		if list == nil {
			t.Fatal("s3 error")
		}
		if len(*list) != 0 {
			t.Fatal("io error")
		}
	})
}
