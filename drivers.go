package gohalt

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

type GinKey func(*gin.Context) interface{}

func GinKeyIP(gctx *gin.Context) interface{} {
	return gctx.ClientIP()
}

type GinOn func(*gin.Context, error)

func GinOnTooManyRequests(gctx *gin.Context, err error) {
	gctx.AbortWithStatus(http.StatusTooManyRequests)
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
