package gohalt

import (
	"context"
	"net/http"
	"strings"

	"github.com/astaxie/beego"
	beegoctx "github.com/astaxie/beego/context"
	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"
	"github.com/kataras/iris/v12"
	"github.com/labstack/echo/v4"
	"github.com/revel/revel"
)

type GinKey func(*gin.Context) interface{}

func GinKeyIP(gctx *gin.Context) interface{} {
	return gctx.ClientIP()
}

type GinOn func(*gin.Context, error)

func GinOnAbort(gctx *gin.Context, err error) {
	_ = gctx.AbortWithError(http.StatusTooManyRequests, err)
}

func NewHandlerGin(ctx context.Context, thr Throttler, key GinKey, on GinOn) gin.HandlerFunc {
	return func(gctx *gin.Context) {
		ctx = WithKey(ctx, key(gctx))
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
			on(gctx, err)
		}
	}
}

type StdHttpKey func(*http.Request) interface{}

func StdHttpKeyIP(req *http.Request) interface{} {
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

type StdHttpOn func(http.ResponseWriter, error)

func StdHttpOnError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusTooManyRequests)
}

func NewStdHttpHandler(ctx context.Context, h http.Handler, thr Throttler, key StdHttpKey, on StdHttpOn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx = WithKey(ctx, key(req))
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
			on(w, err)
		}
	})
}

type EchoKey func(echo.Context) interface{}

func EchoKeyIP(ectx echo.Context) interface{} {
	return ectx.RealIP()
}

type EchoOn func(echo.Context, error) error

func EchoOnError(ectx echo.Context, err error) error {
	return ectx.String(http.StatusTooManyRequests, err.Error())
}

func NewMiddlewareEcho(ctx context.Context, thr Throttler, key EchoKey, on EchoOn) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ectx echo.Context) error {
			ctx = WithKey(ctx, key(ectx))
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				headers := NewMeta(ctx, thr).Headers()
				for key, val := range headers {
					ectx.Response().Header().Set(key, val)
				}
				return next(ectx)
			})
			if err := r.Result(); err != nil {
				return on(ectx, err)
			}
			return nil
		}
	}
}

type BeegoKey func(*beegoctx.Context) interface{}

func BeegoKeyIP(bctx *beegoctx.Context) interface{} {
	return bctx.Input.IP()
}

type BeegoOn func(*beegoctx.Context, error)

func BeegoOnAbort(bctx *beegoctx.Context, err error) {
	bctx.Abort(http.StatusTooManyRequests, err.Error())
}

func NewFilterBeego(ctx context.Context, thr Throttler, key BeegoKey, on BeegoOn) beego.FilterFunc {
	return func(bctx *beegoctx.Context) {
		ctx = WithKey(ctx, key(bctx))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				bctx.Output.Header(key, val)
			}
			return nil
		})
		if err := r.Result(); err != nil {
			on(bctx, err)
		}
	}
}

type KitKey func(interface{}) interface{}

func KitKeyReq(req interface{}) interface{} {
	return req
}

type KitOn func(error) (interface{}, error)

func KitOnError(err error) (interface{}, error) {
	return nil, err
}

func NewMiddlewareKit(ctx context.Context, thr Throttler, key KitKey, on KitOn) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			ctx = WithKey(ctx, key(req))
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				resp, err = next(ctx, req)
				return err
			})
			if err := r.Result(); err != nil {
				return on(err)
			}
			return resp, nil
		}
	}
}

type MuxKey StdHttpKey

func MuxKeyIP(req *http.Request) interface{} {
	return StdHttpKeyIP(req)
}

type MuxOn StdHttpOn

func MuxOnError(w http.ResponseWriter, err error) {
	StdHttpOnError(w, err)
}

func NewMuxHandler(ctx context.Context, h http.Handler, thr Throttler, key MuxKey, on MuxOn) http.Handler {
	return NewStdHttpHandler(ctx, h, thr, StdHttpKey(key), StdHttpOn(on))
}

type RouterKey StdHttpKey

func RouterKeyIP(req *http.Request) interface{} {
	return StdHttpKeyIP(req)
}

type RouterOn StdHttpOn

func RouterOnError(w http.ResponseWriter, err error) {
	StdHttpOnError(w, err)
}

func NewRouterHandler(ctx context.Context, h http.Handler, thr Throttler, key MuxKey, on MuxOn) http.Handler {
	return NewStdHttpHandler(ctx, h, thr, StdHttpKey(key), StdHttpOn(on))
}

type RevealKey func(*revel.Controller) interface{}

func RevealKeyIp(rc *revel.Controller) interface{} {
	return rc.ClientIP
}

type RevealOn func(error) revel.Result

func RevealOnError(rc *revel.Controller, err error) revel.Result {
	result := rc.RenderError(err)
	rc.Response.Status = http.StatusTooManyRequests
	return result
}

func NewRevelFilter(ctx context.Context, thr Throttler, key RevealKey, on RevealOn) revel.Filter {
	return func(rc *revel.Controller, chain []revel.Filter) {
		ctx = WithKey(ctx, key(rc))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				rc.Response.Out.Header().Add(key, val)
			}
			chain[0](rc, chain[1:])
			return nil
		})
		if err := r.Result(); err != nil {
			rc.Result = on(err)
		}
	}
}

type IrisKey func(iris.Context) interface{}

func IrisKeyIP(ictx iris.Context) interface{} {
	return ictx.RemoteAddr()
}

type IrisOn func(iris.Context, error)

func IrisOnError(ictx iris.Context, err error) {
	_, _ = ictx.WriteString(err.Error())
	ictx.StatusCode(http.StatusTooManyRequests)
}

func NewHandlerIris(ctx context.Context, thr Throttler, key IrisKey, on IrisOn) iris.Handler {
	return func(ictx iris.Context) {
		ctx = WithKey(ctx, key(ictx))
		r := NewRunnerSync(ctx, thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				ictx.Header(key, val)
			}
			ictx.Next()
			return nil
		})
		if err := r.Result(); err != nil {
			on(ictx, err)
		}
	}
}
