package gohalt

import (
	"context"

	"github.com/gin-gonic/gin"
)

func NewMiddlewareGin(ctx context.Context, thr Throttler) gin.HandlerFunc {
	return func(gctx *gin.Context) {
		ip := gctx.ClientIP()
		ctx = WithKey(ctx, ip)
		if err := thr.Acquire(ctx); err != nil {
			gctx.Abort()
			return
		}
		defer func() {
			if err := thr.Release(ctx); err != nil {
				gctx.Abort()
				return
			}
		}()
		gctx.Next()
	}
}
