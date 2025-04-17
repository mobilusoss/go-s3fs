package s3fs

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type (
	S3FS struct {
		s3     *s3.Client
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

var ctx = context.TODO()

func New(config *Config) *S3FS {
	if config.Region == "" {
		config.Region = "ap-northeast-1"
	}

	cfg, _ := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(config.Region))

	if config.EnableIAMAuth {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(
			config.AccessKeyID,
			config.AccessSecretKey,
			"",
		)
	}

	serv := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if config.EnableMinioCompat {
			o.UsePathStyle = config.EnableMinioCompat
		}
	})

	if config.Endpoint != "" {
		cfg.BaseEndpoint = aws.String(config.Endpoint)
	}

	return &S3FS{
		serv,
		config,
	}
}

func (s3fs *S3FS) CreateBucket(name string) error {
	_, err := s3fs.s3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return err
	}

	w := s3.NewBucketExistsWaiter(s3fs.s3)

	err = w.Wait(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(name),
	}, 5*time.Minute)
	if err != nil {
		return err
	}

	return err
}

func (s3fs *S3FS) DeleteBucket(name string) error {
	_, err := s3fs.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

func (s3fs *S3FS) List(key string) *[]FileInfo {
	fileList := make([]FileInfo, 0)
	var continuationToken *string
	for {
		list, err := s3fs.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3fs.config.Bucket),
			Prefix:            aws.String(s3fs.getKey(key)),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil
		}
		for _, val := range list.CommonPrefixes {
			if *val.Prefix == s3fs.getKey("") {
				continue
			}

			k := strings.Split(*val.Prefix, "/")
			name := k[len(k)-2]
			path := "/" + strings.TrimPrefix(*val.Prefix, s3fs.getKey(""))
			fileInfo := FileInfo{
				Type: Directory,
				Name: name,
				Path: path,
				//Raw:  val,
			}
			fileList = append(fileList, fileInfo)
		}
		for _, val := range list.Contents {
			if *val.Key == s3fs.getKey("") {
				continue
			}
			if *val.Key == s3fs.getKey(key) {
				continue
			}

			k := strings.Split(*val.Key, "/")
			name := k[len(k)-1]
			path := "/" + strings.TrimPrefix(*val.Key, s3fs.getKey(""))
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

func (s3fs *S3FS) MkDir(key string) error {
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	_, err := s3fs.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3fs.config.Bucket),
		Key:    aws.String(s3fs.getKey(key)),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) Get(key string) (*io.ReadCloser, error) {
	output, err := s3fs.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3fs.config.Bucket),
		Key:    aws.String(s3fs.getKey(key)),
	})
	if err != nil {
		return nil, err
	}
	return &output.Body, nil
}

func (s3fs *S3FS) Put(key string, body io.ReadCloser, contentType string) error {
	uploader := manager.NewUploader(s3fs.s3)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s3fs.config.Bucket),
		Key:         aws.String(s3fs.getKey(key)),
		Body:        body,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) Delete(key string) error {
	if strings.HasSuffix(key, "/") {
		return s3fs.BulkDelete(key)
	} else {
		return s3fs.SingleDelete(key)
	}
}

func (s3fs *S3FS) SingleDelete(key string) error {
	_, err := s3fs.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s3fs.config.Bucket),
		Key:    aws.String(s3fs.getKey(key)),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) BulkDelete(prefix string) error {
	var continuationToken *string
	for {
		list, err := s3fs.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3fs.config.Bucket),
			Prefix:            aws.String(s3fs.getKey(prefix)),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return err
		}

		objects := []types.ObjectIdentifier{}
		for _, content := range list.Contents {
			objects = append(objects, types.ObjectIdentifier{
				Key: content.Key,
			})
		}

		_, err = s3fs.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s3fs.config.Bucket),
			Delete: &types.Delete{
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

func (s3fs *S3FS) Copy(src string, dest string, metadata map[string]string) error {
	if strings.HasSuffix(src, "/") {
		return s3fs.BulkCopy(src, dest, metadata)
	} else {
		return s3fs.SingleCopy(src, dest, metadata)
	}
}

func (s3fs *S3FS) SingleCopy(src string, dest string, metadata map[string]string) error {
	var err error
	if metadata == nil {
		_, err = s3fs.s3.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(s3fs.config.Bucket),
			CopySource: aws.String(url.QueryEscape(s3fs.config.Bucket + "/" + s3fs.getKey(src))),
			Key:        aws.String(s3fs.getKey(dest)),
		})
	} else {
		_, err = s3fs.s3.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(s3fs.config.Bucket),
			CopySource:        aws.String(url.QueryEscape(s3fs.config.Bucket + "/" + s3fs.getKey(src))),
			Key:               aws.String(s3fs.getKey(dest)),
			Metadata:          metadata,
			MetadataDirective: types.MetadataDirectiveReplace,
		})
	}

	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) BulkCopy(prefix string, dest string, metadata map[string]string) error {
	var continuationToken *string
	for {
		list, err := s3fs.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3fs.config.Bucket),
			Prefix:            aws.String(s3fs.getKey(prefix)),
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
			go func(c types.Object) {
				srcRel := strings.Replace(*c.Key, s3fs.config.Domain, "", 1)
				destRel := strings.Replace(dest, s3fs.config.Domain, "", 1)
				targetPath := destRel + strings.TrimPrefix(srcRel, baseKey)

				var e error
				if strings.HasSuffix(srcRel, "/") {
					e = s3fs.MkDir(targetPath)
				} else {
					e = s3fs.SingleCopy(srcRel, targetPath, metadata)
				}

				if e != nil {
					result = e
				}

				wg.Done()
			}(content)
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

func (s3fs *S3FS) Move(src string, dest string) error {
	if strings.HasSuffix(src, "/") {
		return s3fs.BulkMove(src, dest)
	} else {
		return s3fs.SingleMove(src, dest)
	}
}

func (s3fs *S3FS) SingleMove(src string, dest string) error {
	if err := s3fs.Copy(src, dest, nil); err != nil {
		return err
	}
	if err := s3fs.Delete(src); err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) BulkMove(prefix string, dest string) error {
	if err := s3fs.BulkCopy(prefix, dest, nil); err != nil {
		return err
	}
	if err := s3fs.BulkDelete(prefix); err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FS) Info(key string) *s3.HeadObjectOutput {
	result, _ := s3fs.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s3fs.config.Bucket),
		Key:    aws.String(s3fs.getKey(key)),
	})
	return result
}

func (s3fs *S3FS) getKey(key string) string {
	k := ""
	if s3fs.config.NameSpace != "" {
		k += s3fs.config.NameSpace + "/"
	}
	if s3fs.config.Domain != "" {
		k += s3fs.config.Domain + "/"
	}
	key = strings.TrimPrefix(key, "/")

	return k + key
}

func (s3fs *S3FS) PathExists(key string) bool {
	list, err := s3fs.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.config.Bucket),
		Prefix:    aws.String(s3fs.getKey(key)),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1),
	})
	if err != nil {
		return false
	}
	if *list.KeyCount > 0 {
		return true
	}
	return false
}

func (s3fs *S3FS) ExactPathExists(key string) bool {
	list, err := s3fs.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.config.Bucket),
		Prefix:    aws.String(s3fs.getKey(key)),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return false
	}

	if *list.KeyCount == 0 {
		return false
	}

	for _, val := range list.Contents {
		if *val.Key == s3fs.getKey(key) {
			return true
		}
	}

	return false
}
