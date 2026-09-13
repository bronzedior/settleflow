package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

const baseURL = "https://graph.microsoft.com/v1.0"

type Client struct {
	cred azcore.TokenCredential
	http *http.Client
}

func NewClient(cred azcore.TokenCredential) *Client {
	return &Client{cred: cred, http: http.DefaultClient}
}

type Page struct {
	Value    []map[string]any `json:"value"`
	NextLink string           `json:"@odata.nextLink"`
}

func (c *Client) ListPage(ctx context.Context, resource, nextLink string) (Page, error) {
	url := nextLink
	if url == "" {
		url = baseURL + "/" + resource + "?$top=100"
	}

	var page Page
	if err := c.do(ctx, http.MethodGet, url, &page); err != nil {
		return Page{}, err
	}
	return page, nil
}

func (c *Client) GetDetail(ctx context.Context, resource, objectID string) (map[string]any, error) {
	url := fmt.Sprintf("%s/%s/%s?$expand=owners($select=id)", baseURL, resource, objectID)

	var obj map[string]any
	if err := c.do(ctx, http.MethodGet, url, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *Client) do(ctx context.Context, method, url string, out any) error {
	token, err := c.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://graph.microsoft.com/.default"},
	})
	if err != nil {
		return fmt.Errorf("graph: acquire token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return parseAPIError(resp, body)
	}
	return json.Unmarshal(body, out)
}

func parseAPIError(resp *http.Response, body []byte) error {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)

	var retryAfter time.Duration
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			retryAfter = time.Duration(secs) * time.Second
		}
	}

	return &APIError{
		Status:     resp.StatusCode,
		Code:       envelope.Error.Code,
		RetryAfter: retryAfter,
		err:        fmt.Errorf("%s", envelope.Error.Message),
	}
}
