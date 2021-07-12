package sync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const (
	cacheRoot = "/tmp/test-path"
)

func Test_currentFileIndex(t *testing.T) {
	tests := []struct {
		name      string
		fsModFunc func(t *testing.T, fs afero.Fs)
		want      api.CacheEntities
		wantErr   bool
	}{
		{
			name:      "no files, empty index",
			fsModFunc: nil,
			want:      nil,
			wantErr:   false,
		},
		{
			name: "lists cached images",
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestFile(t, fs, cacheRoot+"/ubuntu/19.04/20201025/img.tar.lz4")
				createTestFile(t, fs, cacheRoot+"/ubuntu/19.04/20201026/img.tar.lz4")
				createTestFile(t, fs, cacheRoot+"/ubuntu/19.04/20201025/img.tar.lz4.md5")
				createTestFile(t, fs, cacheRoot+"/ubuntu/19.04/20201026/img.tar.lz4.md5")
				createTestFile(t, fs, cacheRoot+"/ubuntu/20.10/20201026/img.tar.lz4")
				createTestFile(t, fs, cacheRoot+"/ubuntu/20.10/20201026/img.tar.lz4.md5")
			},
			want: api.CacheEntities{
				api.LocalFile{
					Name:    "img.tar.lz4",
					SubPath: "ubuntu/19.04/20201025/img.tar.lz4",
					Size:    4,
				},
				api.LocalFile{
					Name:    "img.tar.lz4",
					SubPath: "ubuntu/19.04/20201026/img.tar.lz4",
					Size:    4,
				},
				api.LocalFile{
					Name:    "img.tar.lz4",
					SubPath: "ubuntu/20.10/20201026/img.tar.lz4",
					Size:    4,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			require.Nil(t, fs.MkdirAll(cacheRoot, 0755))
			if tt.fsModFunc != nil {
				tt.fsModFunc(t, fs)
			}
			got, err := currentFileIndex(fs, cacheRoot)
			if (err != nil) != tt.wantErr {
				t.Errorf("Syncer.currentImageIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, cmpopts.IgnoreUnexported(strfmt.DateTime{})); diff != "" {
				t.Errorf("Syncer.currentImageIndex() diff = %v", diff)
			}
		})
	}
}

func createTestFile(t *testing.T, fs afero.Fs, p string) {
	createTestDir(t, fs, path.Base(p))
	f, err := fs.Create(p)
	require.Nil(t, err)
	defer f.Close()
	_, err = f.WriteString("Test")
	require.Nil(t, err)
}

func createTestDir(t *testing.T, fs afero.Fs, p string) {
	require.Nil(t, fs.MkdirAll(p, 0755))
}

func dlLoggingSvc(data []byte) (*s3.S3, *[]string, *[]string) {
	var m sync.Mutex
	names := []string{}
	ranges := []string{}

	svc := s3.New(unit.Session)
	svc.Handlers.Send.Clear()
	svc.Handlers.Send.PushBack(func(r *request.Request) {
		m.Lock()
		defer m.Unlock()

		names = append(names, r.Operation.Name)
		ranges = append(ranges, *r.Params.(*s3.GetObjectInput).Range)

		rerng := regexp.MustCompile(`bytes=(\d+)-(\d+)`)
		rng := rerng.FindStringSubmatch(r.HTTPRequest.Header.Get("Range"))
		start, _ := strconv.ParseInt(rng[1], 10, 64)
		fin, _ := strconv.ParseInt(rng[2], 10, 64)
		fin++

		if fin > int64(len(data)) {
			fin = int64(len(data))
		}

		bodyBytes := data[start:fin]
		r.HTTPResponse = &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
			Header:     http.Header{},
		}
		r.HTTPResponse.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d",
			start, fin-1, len(data)))
		r.HTTPResponse.Header.Set("Content-Length", fmt.Sprintf("%d", len(bodyBytes)))
	})

	return svc, &names, &ranges
}

func TestSyncer_defineImageDiff(t *testing.T) {
	tests := []struct {
		name               string
		fsModFunc          func(t *testing.T, fs afero.Fs)
		currentImages      api.CacheEntities
		remoteChecksumFile string
		wantImages         api.CacheEntities
		remove             api.CacheEntities
		keep               api.CacheEntities
		add                api.CacheEntities
		wantErr            bool
	}{
		{
			name:          "no current, no want images -> nothing to do",
			currentImages: nil,
			wantImages:    nil,
			add:           nil,
			keep:          nil,
			remove:        nil,
			wantErr:       false,
		},
		{
			name: "remove unexisting images",
			currentImages: api.CacheEntities{
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201026/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
			},
			wantImages: nil,
			add:        nil,
			remove: api.CacheEntities{
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201026/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
			},
			wantErr: false,
		},
		{
			name:          "add new images",
			currentImages: nil,
			wantImages: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
				},
				api.OS{
					Name:       "debian",
					Version:    semver.MustParse("20.04"),
					BucketKey:  "metal-os/master/ubuntu/20.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
				},
			},
			add: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
				},
				api.OS{
					Name:       "debian",
					Version:    semver.MustParse("20.04"),
					BucketKey:  "metal-os/master/ubuntu/20.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
				},
			},
			remove:  nil,
			wantErr: false,
		},
		{
			name: "don't download existing images when checksum is proper",
			currentImages: api.CacheEntities{
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
			},
			wantImages: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04.20201025"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					MD5Ref: s3.Object{
						Key: strPtr("metal-os/master/ubuntu/19.04/20201025/img.tar.lz4.md5"),
					},
				},
			},
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestFile(t, fs, cacheRoot+"/metal-os/master/ubuntu/19.04/20201025/img.tar.lz4")
			},
			add: nil,
			keep: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04.20201025"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					MD5Ref: s3.Object{
						Key: strPtr("metal-os/master/ubuntu/19.04/20201025/img.tar.lz4.md5"),
					},
				},
			},
			remove:  nil,
			wantErr: false,
		},
		{
			name: "download existing images when checksum is incorrect",
			currentImages: api.CacheEntities{
				api.OS{
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					Version:    &semver.Version{},
				},
			},
			wantImages: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04.20201025"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					MD5Ref: s3.Object{
						Key: strPtr("metal-os/master/ubuntu/19.04/20201025/img.tar.lz4.md5"),
					},
				},
			},
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestFile(t, fs, cacheRoot+"/metal-os/master/ubuntu/19.04/20201025/img.tar.lz4")
			},
			remoteChecksumFile: "not-equal",
			add: api.CacheEntities{
				api.OS{
					Name:       "ubuntu",
					Version:    semver.MustParse("19.04.20201025"),
					BucketKey:  "metal-os/master/ubuntu/19.04/20201025/img.tar.lz4",
					BucketName: "metal-os",
					MD5Ref: s3.Object{
						Key: strPtr("metal-os/master/ubuntu/19.04/20201025/img.tar.lz4.md5"),
					},
				},
			},
			remove:  nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			require.Nil(t, fs.MkdirAll(cacheRoot, 0755))
			if tt.fsModFunc != nil {
				tt.fsModFunc(t, fs)
			}

			remoteChecksumFile := "0cbc6611f5540bd0809a388dc95a615b  img.tar.lz4"
			if tt.remoteChecksumFile != "" {
				remoteChecksumFile = tt.remoteChecksumFile
			}

			s3Client, _, _ := dlLoggingSvc([]byte(remoteChecksumFile))
			d := s3manager.NewDownloaderWithClient(s3Client)
			s := &Syncer{
				logger: zaptest.NewLogger(t).Sugar(),
				fs:     fs,
				s3:     d,
				stop:   context.TODO(),
			}

			gotRemove, gotKeep, gotAdd, err := s.defineDiff(cacheRoot, tt.currentImages, tt.wantImages)
			if (err != nil) != tt.wantErr {
				t.Errorf("Syncer.defineImageDiff() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(gotAdd, tt.add, cmpopts.IgnoreUnexported(strfmt.DateTime{})); diff != "" {
				t.Errorf("Syncer.defineImageDiff() add diff = %v", diff)
			}
			if diff := cmp.Diff(gotKeep, tt.keep, cmpopts.IgnoreUnexported(strfmt.DateTime{})); diff != "" {
				t.Errorf("Syncer.defineImageDiff() keep diff = %v", diff)
			}
			if diff := cmp.Diff(gotRemove, tt.remove, cmpopts.IgnoreUnexported(strfmt.DateTime{})); diff != "" {
				t.Errorf("Syncer.defineImageDiff() remove diff = %v", diff)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}

func Test_cleanEmptyDirs(t *testing.T) {
	tests := []struct {
		name        string
		fsModFunc   func(t *testing.T, fs afero.Fs)
		fsCheckFunc func(t *testing.T, fs afero.Fs)
		wantErr     error
	}{
		{
			name:      "no directory contents, nothing happens",
			fsModFunc: nil,
			wantErr:   nil,
		},
		{
			name: "flat deletion",
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestDir(t, fs, cacheRoot+"/ubuntu")
			},
			fsCheckFunc: func(t *testing.T, fs afero.Fs) {
				exists, err := afero.Exists(fs, cacheRoot+"/ubuntu")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")
			},
			wantErr: nil,
		},
		{
			name: "recursive deletion 1",
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestDir(t, fs, cacheRoot+"/ubuntu/20.10/20201027")
			},
			fsCheckFunc: func(t *testing.T, fs afero.Fs) {
				exists, err := afero.Exists(fs, cacheRoot+"/ubuntu/20.10/20201027")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")

				exists, err = afero.Exists(fs, cacheRoot+"/ubuntu/20.10")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")

				exists, err = afero.Exists(fs, cacheRoot+"/ubuntu")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")
			},
			wantErr: nil,
		},
		{
			name: "recursive deletion 2",
			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestFile(t, fs, cacheRoot+"/ubuntu/20.04/20201028/img.tar.lz4")
				createTestDir(t, fs, cacheRoot+"/ubuntu/20.10/20201027")
			},
			fsCheckFunc: func(t *testing.T, fs afero.Fs) {
				exists, err := afero.Exists(fs, cacheRoot+"/ubuntu/20.10/20201027")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")

				exists, err = afero.Exists(fs, cacheRoot+"/ubuntu/20.10")
				assert.NoError(t, err)
				assert.False(t, exists, "dir still exists")

				exists, err = afero.Exists(fs, cacheRoot+"/ubuntu")
				assert.NoError(t, err)
				assert.True(t, exists, "dir was deleted")
			},
			wantErr: nil,
		},
		{
			name: "kind of realistic scenario",

			fsModFunc: func(t *testing.T, fs afero.Fs) {
				createTestDir(t, fs, cacheRoot+"/boot/metal-hammer/releases/download/v0.8.0")
				createTestFile(t, fs, cacheRoot+"/boot/metal-hammer/pull-requests/pr-title/metal-hammer-initrd.img.lz4")
				createTestFile(t, fs, cacheRoot+"/boot/metal-hammer/pull-requests/pr-title/metal-hammer-initrd.img.lz4.md5")
				createTestFile(t, fs, cacheRoot+"/ubuntu/20.10/20201026/img.tar.lz4")
				createTestFile(t, fs, cacheRoot+"/ubuntu/20.10/20201026/img.tar.lz4.md5")
				createTestDir(t, fs, cacheRoot+"/firewall/2.0/20210131")
				createTestDir(t, fs, cacheRoot+"/firewall/2.0/20210207")
				createTestFile(t, fs, cacheRoot+"/firewall/2.0/20210304/img.tar.lz4")
				createTestFile(t, fs, cacheRoot+"/firewall/2.0/20210304/img.tar.lz4.md5")
			},
			fsCheckFunc: func(t *testing.T, fs afero.Fs) {
				for _, subPath := range []string{
					"/boot/metal-hammer/releases",
					"/firewall/2.0.20210131",
					"/firewall/2.0.20210207",
				} {
					exists, err := afero.Exists(fs, cacheRoot+subPath)
					assert.NoError(t, err)
					assert.False(t, exists, "dir still exists")
				}

				for _, subPath := range []string{
					"/boot/metal-hammer/pull-requests/pr-title/metal-hammer-initrd.img.lz4",
					"/boot/metal-hammer/pull-requests/pr-title/metal-hammer-initrd.img.lz4.md5",
					"/ubuntu/20.10/20201026/img.tar.lz4",
					"/ubuntu/20.10/20201026/img.tar.lz4.md5",
					"/firewall/2.0/20210304/img.tar.lz4",
					"/firewall/2.0/20210304/img.tar.lz4.md5",
				} {
					exists, err := afero.Exists(fs, cacheRoot+subPath)
					assert.NoError(t, err)
					assert.True(t, exists, "dir was deleted")
				}
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			require.Nil(t, fs.MkdirAll(cacheRoot, 0755))
			if tt.fsModFunc != nil {
				tt.fsModFunc(t, fs)
			}

			err := cleanEmptyDirs(fs, cacheRoot)
			if diff := cmp.Diff(err, tt.wantErr); diff != "" {
				t.Errorf("cleanEmptyDirs() diff = %v", diff)
			}

			if tt.fsCheckFunc != nil {
				tt.fsCheckFunc(t, fs)
			}
		})
	}
}
