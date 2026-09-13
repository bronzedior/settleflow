package jobs

import "fmt"

func TaskKey(args any) string {
	switch a := args.(type) {
	case DiscoverPageArgs:
		return fmt.Sprintf("page:%s:%s", a.Resource, a.NextLink)
	case DiscoverDetailArgs:
		return "detail:" + a.ObjectID
	}
	panic(fmt.Sprintf("taskKey: unhandled args type %T", args))
}
