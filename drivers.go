package gohalt

import (
	"context"
	"net/http"
	"strings"

	"github.com/astaxie/beego"
	beegoctx "github.com/astaxie/beego/context"
	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"
	"github.com/labstack/echo/v4"
)

type KeyGin func(*gin.Context) interface{}

func KeyGinIP(gctx *gin.Context) interface{} {
	return gctx.ClientIP()
}

type OnGin func(*gin.Context, error)

func OnGinAbort(gctx *gin.Context, err error) {
	_ = gctx.AbortWithError(http.StatusTooManyRequests, err)
}

func NewMiddlewareGin(ctx context.Context, thr Throttler, keyg KeyGin, ong OnGin) gin.HandlerFunc {
	return func(gctx *gin.Context) {
		ctx = WithKey(ctx, keyg(gctx))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				gctx.Header(key, val)
			}
			gctx.Next()
			return nil
		})
		if err := r.Result(); err != nil {
			ong(gctx, err)
		}
	}
}

type KeyStdHttp func(*http.Request) interface{}

func KeyStdHttpIP(req *http.Request) interface{} {
	first := func(ip string) string {
		return strings.TrimSpace(strings.Split(ip, ",")[0])
	}
	if ip := strings.TrimSpace(req.Header.Get("X-Real-Ip")); ip != "" {
		return first(ip)
	}
	if ip := strings.TrimSpace(req.Header.Get("X-Forwarded-For")); ip != "" {
		return first(ip)
	}
	return first(req.RemoteAddr)
}

type OnStdHttp func(http.ResponseWriter, error)

func OnStdHttpError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusTooManyRequests)
}

func NewStdHttpHandler(ctx context.Context, h http.Handler, thr Throttler, keystd KeyStdHttp, onstd OnStdHttp) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx = WithKey(ctx, keystd(req))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				w.Header().Add(key, val)
			}
			h.ServeHTTP(w, req)
			return nil
		})
		if err := r.Result(); err != nil {
			onstd(w, err)
		}
	})
}

type KeyEcho func(echo.Context) interface{}

func KeyEchoIP(ectx echo.Context) interface{} {
	return ectx.RealIP()
}

type OnEcho func(echo.Context, error) error

func OnEchoError(ectx echo.Context, err error) error {
	return ectx.String(http.StatusTooManyRequests, err.Error())
}

func NewMiddlewareEcho(ctx context.Context, thr Throttler, keye KeyEcho, one OnEcho) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ectx echo.Context) error {
			ctx = WithKey(ctx, keye(ectx))
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				headers := NewMeta(ctx, thr).Headers()
				for key, val := range headers {
					ectx.Response().Header().Set(key, val)
				}
				return next(ectx)
			})
			if err := r.Result(); err != nil {
				return one(ectx, err)
			}
			return nil
		}
	}
}

type KeyBeego func(*beegoctx.Context) interface{}

func KeyBeegoIP(bctx *beegoctx.Context) interface{} {
	return bctx.Input.IP()
}

type OnBeego func(*beegoctx.Context, error)

func OnBeegoAbort(bctx *beegoctx.Context, err error) {
	bctx.Abort(http.StatusTooManyRequests, err.Error())
}

func NewMiddlewareBeego(ctx context.Context, thr Throttler, keyb KeyBeego, onb OnBeego) beego.FilterFunc {
	return func(bctx *beegoctx.Context) {
		ctx = WithKey(ctx, keyb(bctx))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				bctx.Output.Header(key, val)
			}
			return nil
		})
		if err := r.Result(); err != nil {
			onb(bctx, err)
		}
	}
}

type KeyKit func(interface{}) interface{}

func KeyKitReq(req interface{}) interface{} {
	return req
}

type OnKit func(error) (interface{}, error)

func OnKitEcho(err error) (interface{}, error) {
	return nil, err
}

func NewMiddlewareKit(ctx context.Context, thr Throttler, keyk KeyKit, onk OnKit) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			ctx = WithKey(ctx, keyk(req))
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				resp, err = next(ctx, req)
				return err
			})
			if err := r.Result(); err != nil {
				return onk(err)
			}
			return resp, nil
		}
	}
}
