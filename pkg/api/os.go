package api

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/metal-stack/metal-go/api/models"
)

type OS struct {
	Name      string
	Version   *semver.Version
	ApiRef    models.V1ImageResponse
	ImageRef  s3.Object
	MD5Ref    s3.Object
	BucketKey string
}
type OSImagesByVersion map[string][]OS
type OSImagesByOS map[string]OSImagesByVersion

func SortOSImagesByName(imgs []OS) {
	sort.Slice(imgs, func(i, j int) bool {
		if imgs[i].Name == imgs[j].Name {
			return imgs[i].Version.LessThan(imgs[j].Version)
		}
		return strings.Compare(imgs[i].Name, imgs[j].Name) < 0
	})
}

func (o *OS) MajorMinor() (string, error) {
	if o.Version == nil {
		return "", fmt.Errorf("image version is nil")
	}
	return fmt.Sprintf("%d.%d", o.Version.Major(), o.Version.Minor()), nil
}
