package gohalt

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
)

type GinKey func(*gin.Context) interface{}

func GinKeyIP(gctx *gin.Context) interface{} {
	return gctx.ClientIP()
}

type GinOn func(*gin.Context, error)

func GinOnTooManyRequests(gctx *gin.Context, err error) {
	gctx.AbortWithError(http.StatusTooManyRequests, err)
}

func NewMiddlewareGin(ctx context.Context, thr Throttler, gkey GinKey, gon GinOn) gin.HandlerFunc {
	return func(gctx *gin.Context) {
		ctx = WithKey(ctx, gkey(gctx))
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
			gon(gctx, err)
		}
	}
}

type StdHttpKey func(*http.Request) interface{}

func StdHttpIP(req *http.Request) interface{} {
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

func StdHttpOnTooManyRequests(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusTooManyRequests)
}

func NewStdHttpHandler(ctx context.Context, h http.Handler, thr Throttler, shkey StdHttpKey, shon StdHttpOn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx = WithKey(ctx, shkey(req))
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
			shon(w, err)
		}
	})
}

type EchoKey func(echo.Context) interface{}

func EchoKeyIP(ectx echo.Context) interface{} {
	return ectx.RealIP()
}

type EchoOn func(echo.Context, error) error

func EchoOnTooManyRequests(ectx echo.Context, err error) error {
	ectx.String(http.StatusTooManyRequests, err.Error())
	return err
}

func RecoverWithConfig(ctx context.Context, thr Throttler, ekey EchoKey, eon EchoOn) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ectx echo.Context) error {
			ctx = WithKey(ctx, ekey(ectx))
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				headers := NewMeta(ctx, thr).Headers()
				for key, val := range headers {
					ectx.Response().Header().Set(key, val)
				}
				return next(ectx)
			})
			if err := r.Result(); err != nil {
				return eon(ectx, err)
			}
			return nil
		}
	}
}
