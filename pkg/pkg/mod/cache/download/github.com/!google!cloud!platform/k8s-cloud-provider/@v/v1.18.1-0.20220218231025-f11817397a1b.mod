module github.com/GoogleCloudPlatform/k8s-cloud-provider

go 1.13

require (
	golang.org/x/oauth2 v0.0.0-20211005180243-6b3c2da341f1
	google.golang.org/api v0.60.0
	k8s.io/klog/v2 v2.0.0
)

replace (
	cloud.google.com/go => cloud.google.com/go v0.51.0
	golang.org/x/net => golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/oauth2 => golang.org/x/oauth2 v0.0.0-20211005180243-6b3c2da341f1
	golang.org/x/sys => golang.org/x/sys v0.0.0-20200116001909-b77594299b42
	google.golang.org/api => google.golang.org/api v0.60.0
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20210909211513-a8c4777a87af
)
