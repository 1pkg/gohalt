package gohalt

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type searcher func(ctx context.Context, q string) (string, error)

func search(ctx context.Context, s searcher, topics []string) (urls []string) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	urls = make([]string, 0, len(topics))
	for _, topic := range topics {
		select {
		case <-ctx.Done():
			return urls
		default:
		}
		topic := topic
		go func() {
			url, err := s(ctx, topic)
			if err != nil {
				cancel()
				return
			}
			urls = append(urls, url)
		}()
	}
	return urls
}

func maxsearch(ctx context.Context, s searcher, topics []string, max uint64) (urls []string) {
	ctx = WithThrottler(ctx, NewThrottlerRunning(max), time.Millisecond)
	return search(ctx, s, topics)
}

func duckduckgo(ctx context.Context, q string) (string, error) {
	var answer struct {
		AbstractURL string `json:"AbstractURL"`
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	resp, err := http.Get(fmt.Sprintf("http://api.duckduckgo.com/?q=%s&format=json", q))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", err
	}
	return answer.AbstractURL, nil
}

func maxsearcher(s searcher, max uint64) searcher {
	t := NewThrottlerRunning(max)
	return func(ctx context.Context, q string) (string, error) {
		if err := t.Acquire(ctx); err != nil {
			return "", err
		}
		defer func() {
			if err := t.Release(ctx); err != nil {
				log.Print(err.Error())
			}
		}()
		return s(ctx, q)
	}
}

func maxrunsearcher(s searcher, max uint64) searcher {
	t := NewThrottlerBuffered(max)
	return func(ctx context.Context, q string) (string, error) {
		var result string
		r := NewRunnerSync(ctx, t)
		r.Run(func(ctx context.Context) (err error) {
			result, err = s(ctx, q)
			return
		})
		return result, r.Result()
	}
}
