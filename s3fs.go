package s3fs

import (
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type (
	S3FS struct {
		sess   *session.Session
		s3     *s3.S3
		config *Config
	}
	Config struct {
		NameSpace         string
		Domain            string
		Region            string
		Bucket            string
		EnableIAMAuth     bool
		AccessKeyID       string
		AccessSecretKey   string
		EnableMinioCompat bool
		Endpoint          string
	}
	FileInfo struct {
		Name string `json:"name"`
		Path string `json:"path"`
		Type int    `json:"type"`
		Size int64  `json:"size,omitempty"`
		//Raw  interface{} `json:"raw"`
	}
	CopyInfo struct {
		Src  string
		Dest string
	}
)

const (
	Directory int = 1 + iota
	File
)

func New(config *Config) *S3FS {
	if config.Region == "" {
		config.Region = endpoints.ApNortheast1RegionID
	}

	options := session.Options{
		Config: aws.Config{
			Region: aws.String(config.Region),
		},
	}
	if config.EnableIAMAuth {
		options.Config.Credentials = credentials.NewStaticCredentials(config.AccessKeyID, config.AccessSecretKey, "")
		options.SharedConfigState = session.SharedConfigDisable
	}

	if config.EnableMinioCompat {
		options.Config.S3ForcePathStyle = aws.Bool(config.EnableMinioCompat)
	}

	if config.Endpoint != "" {
		options.Config.Endpoint = aws.String(config.Endpoint)
	}

	sess := session.Must(session.NewSessionWithOptions(options))
	serv := s3.New(sess)

	return &S3FS{
		sess,
		serv,
		config,
	}
}

func (this *S3FS) CreateBucket(name string) error {
	_, err := this.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return err
	}

	err = this.s3.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

func (this *S3FS) DeleteBucket(name string) error {
	_, err := this.s3.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

func (this *S3FS) List(key string) *[]FileInfo {
	fileList := make([]FileInfo, 0)
	var continuationToken *string
	for {
		list, err := this.s3.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(this.config.Bucket),
			Prefix:            aws.String(this.getKey(key)),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil
		}
		for _, val := range list.CommonPrefixes {
			if *val.Prefix == this.getKey("") {
				continue
			}

			k := strings.Split(*val.Prefix, "/")
			name := k[len(k)-2]
			path := "/" + strings.TrimPrefix(*val.Prefix, this.getKey(""))
			fileInfo := FileInfo{
				Type: Directory,
				Name: name,
				Path: path,
				//Raw:  val,
			}
			fileList = append(fileList, fileInfo)
		}
		for _, val := range list.Contents {
			if *val.Key == this.getKey("") {
				continue
			}
			if *val.Key == this.getKey(key) {
				continue
			}

			k := strings.Split(*val.Key, "/")
			name := k[len(k)-1]
			path := "/" + strings.TrimPrefix(*val.Key, this.getKey(""))
			fileInfo := FileInfo{
				Type: File,
				Name: name,
				Path: path,
				Size: *val.Size,
				//Raw:  val,
			}
			fileList = append(fileList, fileInfo)
		}

		if *list.IsTruncated {
			continuationToken = list.ContinuationToken
		} else {
			break
		}
	}

	return &fileList
}

func (this *S3FS) MkDir(key string) error {
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	_, err := this.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(this.config.Bucket),
		Key:    aws.String(this.getKey(key)),
	})
	if err != nil {
		return err
	}
	return nil
}

func (this *S3FS) Get(key string) (*io.ReadCloser, error) {
	output, err := this.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(this.config.Bucket),
		Key:    aws.String(this.getKey(key)),
	})
	if err != nil {
		return nil, err
	}
	return &output.Body, nil
}

func (this *S3FS) Put(key string, body io.ReadCloser, contentType string) error {
	uploader := s3manager.NewUploader(this.sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(this.config.Bucket),
		Key:         aws.String(this.getKey(key)),
		Body:        body,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return err
	}
	return nil
}

func (this *S3FS) Delete(key string) error {
	if strings.HasSuffix(key, "/") {
		return this.BulkDelete(key)
	} else {
		return this.SingleDelete(key)
	}
}

func (this *S3FS) SingleDelete(key string) error {
	_, err := this.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(this.config.Bucket),
		Key:    aws.String(this.getKey(key)),
	})
	if err != nil {
		return err
	}
	return nil
}

func (this *S3FS) BulkDelete(prefix string) error {
	var continuationToken *string
	for {
		list, err := this.s3.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(this.config.Bucket),
			Prefix:            aws.String(this.getKey(prefix)),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return err
		}

		objects := []*s3.ObjectIdentifier{}
		for _, content := range list.Contents {
			objects = append(objects, &s3.ObjectIdentifier{
				Key: content.Key,
			})
		}

		_, err = this.s3.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(this.config.Bucket),
			Delete: &s3.Delete{
				Objects: objects,
			},
		})

		if err != nil {
			return err
		}
		if *list.IsTruncated {
			continuationToken = list.ContinuationToken
		} else {
			return nil
		}
	}
}

func (this *S3FS) Copy(src string, dest string, metadata *map[string]*string) error {
	if strings.HasSuffix(src, "/") {
		return this.BulkCopy(src, dest, metadata)
	} else {
		return this.SingleCopy(src, dest, metadata)
	}
}

func (this *S3FS) SingleCopy(src string, dest string, metadata *map[string]*string) error {
	var err error
	if metadata == nil {
		_, err = this.s3.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(this.config.Bucket),
			CopySource: aws.String(url.QueryEscape(this.config.Bucket + "/" + this.getKey(src))),
			Key:        aws.String(this.getKey(dest)),
		})
	} else {
		_, err = this.s3.CopyObject(&s3.CopyObjectInput{
			Bucket:            aws.String(this.config.Bucket),
			CopySource:        aws.String(url.QueryEscape(this.config.Bucket + "/" + this.getKey(src))),
			Key:               aws.String(this.getKey(dest)),
			Metadata:          *metadata,
			MetadataDirective: aws.String(s3.MetadataDirectiveReplace),
		})
	}

	if err != nil {
		return err
	}
	return nil
}

func (this *S3FS) BulkCopy(prefix string, dest string, metadata *map[string]*string) error {
	var continuationToken *string
	for {
		list, err := this.s3.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(this.config.Bucket),
			Prefix:            aws.String(this.getKey(prefix)),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return err
		}

		k := strings.Split(prefix, "/")
		currentKey := k[len(k)-1]
		baseKey := strings.TrimSuffix(prefix, currentKey+"/")

		var result error
		wg := &sync.WaitGroup{}
		for _, content := range list.Contents {
			wg.Add(1)
			go func(c s3.Object) {
				srcRel := strings.Replace(*c.Key, this.config.Domain, "", 1)
				destRel := strings.Replace(dest, this.config.Domain, "", 1)
				targetPath := destRel + strings.TrimPrefix(srcRel, baseKey)

				var e error
				if strings.HasSuffix(srcRel, "/") {
					e = this.MkDir(targetPath)
				} else {
					e = this.SingleCopy(srcRel, targetPath, metadata)
				}

				if e != nil {
					result = e
				}

				wg.Done()
			}(*content)
		}
		wg.Wait()

		if result != nil {
			return errors.New("some files failed")
		}

		if *list.IsTruncated {
			continuationToken = list.ContinuationToken
		} else {
			return nil
		}
	}
}

func (this *S3FS) Move(src string, dest string) error {
	if strings.HasSuffix(src, "/") {
		return this.BulkMove(src, dest)
	} else {
		return this.SingleMove(src, dest)
	}
}

func (this *S3FS) SingleMove(src string, dest string) error {
	if err := this.Copy(src, dest, nil); err != nil {
		return err
	}
	if err := this.Delete(src); err != nil {
		return err
	}
	return nil
}

func (this *S3FS) BulkMove(prefix string, dest string) error {
	if err := this.BulkCopy(prefix, dest, nil); err != nil {
		return err
	}
	if err := this.BulkDelete(prefix); err != nil {
		return err
	}
	return nil
}

func (this *S3FS) Info(key string) *s3.HeadObjectOutput {
	result, _ := this.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(this.config.Bucket),
		Key:    aws.String(this.getKey(key)),
	})
	return result
}

func (this *S3FS) getKey(key string) string {
	k := ""
	if this.config.NameSpace != "" {
		k += this.config.NameSpace + "/"
	}
	if this.config.Domain != "" {
		k += this.config.Domain + "/"
	}
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	return k + key
}

func (this *S3FS) PathExists(key string) bool {
	list, err := this.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(this.config.Bucket),
		Prefix:    aws.String(this.getKey(key)),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1),
	})
	if err != nil {
		return false
	}
	if *list.KeyCount > 0 {
		return true
	}
	return false
}

func (this *S3FS) ExactPathExists(key string) bool {
	list, err := this.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(this.config.Bucket),
		Prefix:    aws.String(this.getKey(key)),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return false
	}

	if *list.KeyCount == 0 {
		return false
	}

	for _, val := range list.Contents {
		if *val.Key == this.getKey(key) {
			return true
		}
	}

	return false
}
