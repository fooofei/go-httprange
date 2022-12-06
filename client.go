package httprange

import "net/http"

type Requester interface {
	Do(r *http.Request) (*http.Response, error)
}
