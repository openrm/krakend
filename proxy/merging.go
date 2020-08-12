package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/devopsfaith/krakend/config"
)

// NewMergeDataMiddleware creates proxy middleware for merging responses from several backends
func NewMergeDataMiddleware(endpointConfig *config.EndpointConfig) Middleware {
	totalBackends := len(endpointConfig.Backend)
	if totalBackends == 0 {
		panic(ErrNoBackends)
	}
	if totalBackends == 1 {
		return EmptyMiddleware
	}
	serviceTimeout := time.Duration(85*endpointConfig.Timeout.Nanoseconds()/100) * time.Nanosecond
	combiner := getResponseCombiner(endpointConfig.ExtraConfig)

	return func(next ...Proxy) Proxy {
		if len(next) != totalBackends {
			panic(ErrNotEnoughProxies)
		}

		if !shouldRunSequentialMerger(endpointConfig) {
			return parallelMerge(serviceTimeout, combiner, next...)
		}

		patterns := make([]string, len(endpointConfig.Backend))
		for i, b := range endpointConfig.Backend {
			patterns[i] = b.URLPattern
		}
		return sequentialMerge(patterns, serviceTimeout, combiner, next...)
	}
}

func shouldRunSequentialMerger(endpointConfig *config.EndpointConfig) bool {
	if v, ok := endpointConfig.ExtraConfig[Namespace]; ok {
		if e, ok := v.(map[string]interface{}); ok {
			if v, ok := e[isSequentialKey]; ok {
				c, ok := v.(bool)
				return ok && c
			}
		}
	}
	return false
}

func parallelMerge(timeout time.Duration, rc ResponseCombiner, next ...Proxy) Proxy {
	return func(ctx context.Context, request *Request) (*Response, error) {
		localCtx, cancel := context.WithTimeout(ctx, timeout)

		parts := make(chan *Response, len(next))
		failed := make(chan error, len(next))

		for _, n := range next {
			go requestPart(localCtx, n, request, parts, failed)
		}

		acc := newIncrementalMergeAccumulator(len(next), rc)
		for i := 0; i < len(next); i++ {
			select {
			case err := <-failed:
				acc.Merge(nil, err)
			case response := <-parts:
				acc.Merge(response, nil)
			}
		}

		result, err := acc.Result()
		cancel()
		return result, err
	}
}

var reMergeKey = regexp.MustCompile(`\{\{\.Resp(\d+)_([\d\w-_\.]+)\}\}`)

func isBlocking(i int, deps [][]int) bool {
	for _, dep := range deps {
		for _, j := range dep {
			if i == j {
				return true
			}
		}
	}
	return false
}

func sequentialMerge(patterns []string, timeout time.Duration, rc ResponseCombiner, next ...Proxy) Proxy {
	deps := make([][]int, len(patterns))
	matches := make([][][]string, len(patterns))
	for i, pattern := range patterns {
		matches[i] = reMergeKey.FindAllStringSubmatch(pattern, -1)
		deps[i] = make([]int, 0, len(matches))
		for _, match := range matches[i] {
			if rNum, err := strconv.Atoi(match[1]); err == nil {
				var found bool
				for _, j := range deps[i] {
					if j == rNum {
						found = true
					}
				}
				if !found {
					deps[i] = append(deps[i], rNum)
				}
			}
		}
	}
	return func(ctx context.Context, request *Request) (*Response, error) {
		localCtx, cancel := context.WithTimeout(ctx, timeout)

		parts := make([]*Response, len(next))
		out := make(chan *Response, 1)
		errCh := make(chan error, 1)

		type partResult struct { i int; err error }
		ch := make(chan partResult, len(next))
		done := make([]chan struct {}, len(next))
		for i := range next {
			done[i] = make(chan struct {}, 1)
		}

		acc := newIncrementalMergeAccumulator(len(next), rc)

		for i := range next {
			go func(i int) {
				n := next[i]
				for _, j := range deps[i] {
					select{
					case <-done[j]:
					case <-localCtx.Done():
						return
					}
				}
				var req *Request = request
				if i > 0 {
					req = CloneRequest(request)
					for _, match := range matches[i] {
						if len(match) > 1 {
							rNum, err := strconv.Atoi(match[1])
							if err != nil || rNum >= i || parts[rNum] == nil {
								continue
							}
							key := "Resp" + match[1] + "_" + match[2]

							var v interface{}
							var ok bool

							data := parts[rNum].Data
							keys := strings.Split(match[2], ".")
							if len(keys) > 1 {
								for _, k := range keys[:len(keys)-1] {
									v, ok = data[k]
									if !ok {
										break
									}
									switch clean := v.(type) {
									case map[string]interface{}:
										data = clean
									default:
										break
									}
								}
							}

							v, ok = data[keys[len(keys)-1]]
							if !ok {
								continue
							}
							switch clean := v.(type) {
							case string:
								req.Params[key] = clean
							case int:
								req.Params[key] = strconv.Itoa(clean)
							case float64:
								req.Params[key] = strconv.FormatFloat(clean, 'E', -1, 32)
							case bool:
								req.Params[key] = strconv.FormatBool(clean)
							default:
								req.Params[key] = fmt.Sprintf("%v", v)
							}
						}
					}
				}
				requestPart(localCtx, n, req, out, errCh)
				select {
				case err := <-errCh:
					ch <- partResult{i, err}
				case response := <-out:
					parts[i] = response
					close(done[i])
					if !response.IsComplete {
						cancel()
						return
					}
					ch <- partResult{i, nil}
				}
			}(i)
		}

		for _ = range next {
			select {
			case res := <-ch:
				if i, err := res.i, res.err; err != nil {
					acc.Merge(nil, err)
					if isBlocking(i, deps) {
						cancel()
						break
					}
				}
			case <-localCtx.Done():
				break
			}
		}

		for _, part := range parts {
			if part != nil {
				acc.Merge(part, nil)
			}
		}

		result, err := acc.Result()
		cancel()
		return result, err
	}
}

type incrementalMergeAccumulator struct {
	pending  int
	data     *Response
	combiner ResponseCombiner
	errs     []error
}

func newIncrementalMergeAccumulator(total int, combiner ResponseCombiner) *incrementalMergeAccumulator {
	return &incrementalMergeAccumulator{
		pending:  total,
		combiner: combiner,
		errs:     []error{},
	}
}

func (i *incrementalMergeAccumulator) Merge(res *Response, err error) {
	i.pending--
	if err != nil {
		i.errs = append(i.errs, err)
		if i.data != nil {
			i.data.IsComplete = false
		}
		return
	}
	if res == nil {
		i.errs = append(i.errs, errNullResult)
		return
	}
	if i.data == nil {
		i.data = res
		return
	}
	i.data = i.combiner(2, []*Response{i.data, res})
}

func (i *incrementalMergeAccumulator) Result() (*Response, error) {
	if i.data == nil {
		err := newMergeError(i.errs)

		// none succeeded
		if len(i.errs) == 1 {
			return nil, err
		}

		return &Response{Data: make(map[string]interface{}, 0), IsComplete: false}, err
	}

	if i.pending != 0 || len(i.errs) != 0 {
		i.data.IsComplete = false
	}
	return i.data, newMergeError(i.errs)
}

func requestPart(ctx context.Context, next Proxy, request *Request, out chan<- *Response, failed chan<- error) {
	localCtx, cancel := context.WithCancel(ctx)

	in, err := next(localCtx, request)
	if err != nil {
		failed <- err
		cancel()
		return
	}
	if in == nil {
		failed <- errNullResult
		cancel()
		return
	}
	select {
	case out <- in:
	case <-ctx.Done():
		failed <- ctx.Err()
	}
	cancel()
}

func newMergeError(errs []error) error {
	if len(errs) == 0 {
		return nil
	} else if len(errs) == 1 {
		return errs[0]
	}
	return mergeError{errs}
}

type mergeError struct {
	errs []error
}

func (m mergeError) Error() string {
	msg := make([]string, len(m.errs))
	for i, err := range m.errs {
		msg[i] = err.Error()
	}
	return strings.Join(msg, "\n")
}

// ResponseCombiner func to merge the collected responses into a single one
type ResponseCombiner func(int, []*Response) *Response

// RegisterResponseCombiner adds a new response combiner into the internal register
func RegisterResponseCombiner(name string, f ResponseCombiner) {
	responseCombiners.SetResponseCombiner(name, f)
}

const (
	mergeKey            = "combiner"
	isSequentialKey     = "sequential"
	defaultCombinerName = "default"
)

var responseCombiners = initResponseCombiners()

func initResponseCombiners() *combinerRegister {
	return newCombinerRegister(map[string]ResponseCombiner{defaultCombinerName: combineData}, combineData)
}

func getResponseCombiner(extra config.ExtraConfig) ResponseCombiner {
	combiner, _ := responseCombiners.GetResponseCombiner(defaultCombinerName)
	if v, ok := extra[Namespace]; ok {
		if e, ok := v.(map[string]interface{}); ok {
			if v, ok := e[mergeKey]; ok {
				if c, ok := responseCombiners.GetResponseCombiner(v.(string)); ok {
					combiner = c
				}
			}
		}
	}
	return combiner
}

func combineData(total int, parts []*Response) *Response {
	isComplete := len(parts) == total
	var retResponse *Response
	for _, part := range parts {
		if part == nil || part.Data == nil {
			isComplete = false
			continue
		}
		isComplete = isComplete && part.IsComplete
		if retResponse == nil {
			retResponse = part
			continue
		}
		for k, v := range part.Data {
			retResponse.Data[k] = v
		}
	}

	if nil == retResponse {
		// do not allow nil data in the response:
		return &Response{Data: make(map[string]interface{}, 0), IsComplete: isComplete}
	}
	retResponse.IsComplete = isComplete
	return retResponse
}
