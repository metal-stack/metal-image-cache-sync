module github.com/metal-stack/metal-image-cache-sync

go 1.15

require (
	cloud.google.com/go/storage v1.12.0
	github.com/Masterminds/semver v1.5.0
	github.com/aws/aws-sdk-go v1.35.35
	github.com/docker/go-units v0.4.0
	github.com/google/go-cmp v0.5.4
	github.com/metal-stack/go-hal v0.3.0
	github.com/metal-stack/metal-api v0.11.4
	github.com/metal-stack/metal-go v0.11.2
	github.com/metal-stack/v v1.0.2
	github.com/olekukonko/tablewriter v0.0.4
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.1
	go.uber.org/zap v1.16.0
	google.golang.org/api v0.35.0
	sigs.k8s.io/controller-runtime v0.6.4
)
