package gohalt

import (
	"context"
	"fmt"
	"net/http"
	"net/rpc"
	"strings"

	"github.com/astaxie/beego"
	beegoctx "github.com/astaxie/beego/context"
	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"
	"github.com/kataras/iris/v12"
	"github.com/labstack/echo/v4"
	"github.com/micro/go-micro/v2/client"
	"github.com/micro/go-micro/v2/server"
	"github.com/revel/revel"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ip(req *http.Request) interface{} {
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

type GinWith func(*gin.Context) context.Context

func GinWithIP(gctx *gin.Context) context.Context {
	return WithKey(gctx.Request.Context(), gctx.ClientIP())
}

type GinOn func(*gin.Context, error)

func GinOnAbort(gctx *gin.Context, err error) {
	_ = gctx.AbortWithError(http.StatusTooManyRequests, err)
}

func NewMiddlewareGin(thr Throttler, with GinWith, on GinOn) gin.HandlerFunc {
	return func(gctx *gin.Context) {
		r := NewRunnerSync(with(gctx), thr)
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

type StdWith func(*http.Request) context.Context

func StdWithIP(req *http.Request) context.Context {
	return WithKey(req.Context(), ip(req))
}

type StdOn func(http.ResponseWriter, error)

func StdOnAbort(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusTooManyRequests)
}

func NewMiddlewareStd(h http.Handler, thr Throttler, with StdWith, on StdOn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r := NewRunnerSync(with(req), thr)
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

type EchoWith func(echo.Context) context.Context

func EchoWithIP(ectx echo.Context) context.Context {
	return WithKey(ectx.Request().Context(), ectx.RealIP())
}

type EchoOn func(echo.Context, error) error

func EchoOnAbort(ectx echo.Context, err error) error {
	return ectx.String(http.StatusTooManyRequests, err.Error())
}

func NewMiddlewareEcho(thr Throttler, with EchoWith, on EchoOn) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ectx echo.Context) (err error) {
			r := NewRunnerSync(with(ectx), thr)
			r.Run(func(ctx context.Context) error {
				headers := NewMeta(ctx, thr).Headers()
				for key, val := range headers {
					ectx.Response().Header().Set(key, val)
				}
				err = next(ectx)
				return nil
			})
			if err := r.Result(); err != nil {
				return on(ectx, err)
			}
			return err
		}
	}
}

type BeegoWith func(*beegoctx.Context) context.Context

func BeegoWithIP(bctx *beegoctx.Context) context.Context {
	return WithKey(bctx.Request.Context(), bctx.Input.IP())
}

type BeegoOn func(*beegoctx.Context, error)

func BeegoOnAbort(bctx *beegoctx.Context, err error) {
	bctx.Abort(http.StatusTooManyRequests, err.Error())
}

func NewMiddlewareBeego(thr Throttler, with BeegoWith, on BeegoOn) beego.FilterFunc {
	return func(bctx *beegoctx.Context) {
		r := NewRunnerSync(with(bctx), thr)
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

type KitWith func(context.Context, interface{}) context.Context

func KitWithNil(ctx context.Context, req interface{}) context.Context {
	return ctx
}

type KitOn func(error) (interface{}, error)

func KitOnAbort(err error) (interface{}, error) {
	return nil, fmt.Errorf("%d: %w", http.StatusTooManyRequests, err)
}

func NewMiddlewareKit(thr Throttler, with KitWith, on KitOn) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			r := NewRunnerSync(with(ctx, req), thr)
			r.Run(func(ctx context.Context) error {
				resp, err = next(ctx, req)
				return nil
			})
			if err := r.Result(); err != nil {
				return on(err)
			}
			return resp, nil
		}
	}
}

type MuxWith StdWith

func MuxWithIP(req *http.Request) context.Context {
	return StdWithIP(req)
}

type MuxOn StdOn

func MuxOnAbort(w http.ResponseWriter, err error) {
	StdOnAbort(w, err)
}

func NewMiddlewareMux(h http.Handler, thr Throttler, with MuxWith, on MuxOn) http.Handler {
	return NewMiddlewareStd(h, thr, StdWith(with), StdOn(on))
}

type RouterWith StdWith

func RouterWithIP(req *http.Request) context.Context {
	return StdWithIP(req)
}

type RouterOn StdOn

func RouterOnAbort(w http.ResponseWriter, err error) {
	StdOnAbort(w, err)
}

func NewMiddlewareRouter(h http.Handler, thr Throttler, with MuxWith, on MuxOn) http.Handler {
	return NewMiddlewareStd(h, thr, StdWith(with), StdOn(on))
}

type RevealWith func(*revel.Controller) context.Context

func RevealWithIp(rc *revel.Controller) context.Context {
	return WithKey(rc.Request.Context(), rc.ClientIP)
}

type RevealOn func(error) revel.Result

func RevealOnAbort(rc *revel.Controller, err error) revel.Result {
	result := rc.RenderError(err)
	rc.Response.Status = http.StatusTooManyRequests
	return result
}

func NewMiddlewareRevel(thr Throttler, with RevealWith, on RevealOn) revel.Filter {
	return func(rc *revel.Controller, chain []revel.Filter) {
		r := NewRunnerSync(with(rc), thr)
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

type IrisWith func(iris.Context) context.Context

func IrisWithIP(ictx iris.Context) context.Context {
	return WithKey(ictx.Request().Context(), ictx.RemoteAddr())
}

type IrisOn func(iris.Context, error)

func IrisOnAbort(ictx iris.Context, err error) {
	ictx.StatusCode(http.StatusTooManyRequests)
	_, _ = ictx.WriteString(err.Error())
}

func NewMiddlewareIris(thr Throttler, with IrisWith, on IrisOn) iris.Handler {
	return func(ictx iris.Context) {
		r := NewRunnerSync(with(ictx), thr)
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

type FastWith func(*fasthttp.RequestCtx) context.Context

func FastWithIP(fctx *fasthttp.RequestCtx) context.Context {
	return WithKey(context.Background(), fctx.RemoteIP())
}

type FastOn func(*fasthttp.RequestCtx, error)

func FastOnAbort(fctx *fasthttp.RequestCtx, err error) {
	fctx.Error(err.Error(), fasthttp.StatusTooManyRequests)
}

func NewMiddlewareFast(h fasthttp.RequestHandler, thr Throttler, with FastWith, on FastOn) fasthttp.RequestHandler {
	return func(fctx *fasthttp.RequestCtx) {
		r := NewRunnerSync(with(fctx), thr)
		r.Run(func(ctx context.Context) error {
			headers := NewMeta(ctx, thr).Headers()
			for key, val := range headers {
				fctx.Response.Header.Add(key, val)
			}
			h(fctx)
			return nil
		})
		if err := r.Result(); err != nil {
			on(fctx, err)
		}
	}
}

type stdrt struct {
	http.RoundTripper
	ctx context.Context
	thr Throttler
}

func NewStdRoundTripper(rt http.RoundTripper, ctx context.Context, thr Throttler) stdrt {
	return stdrt{RoundTripper: rt, ctx: ctx, thr: thr}
}

func (rt stdrt) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r := NewRunnerSync(rt.ctx, rt.thr)
	r.Run(func(ctx context.Context) error {
		headers := NewMeta(rt.ctx, rt.thr).Headers()
		for key, val := range headers {
			req.Header.Add(key, val)
		}
		resp, err = rt.RoundTripper.RoundTrip(req)
		return nil
	})
	if err := r.Result(); err != nil {
		return nil, err
	}
	return resp, err
}

type FastClient interface {
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}

type fastcli struct {
	FastClient
	ctx context.Context
	thr Throttler
}

func NewFastClient(cli FastClient, ctx context.Context, thr Throttler) fastcli {
	return fastcli{FastClient: cli, ctx: ctx, thr: thr}
}

func (cli fastcli) Do(req *fasthttp.Request, resp *fasthttp.Response) (err error) {
	r := NewRunnerSync(cli.ctx, cli.thr)
	r.Run(func(ctx context.Context) error {
		headers := NewMeta(cli.ctx, cli.thr).Headers()
		for key, val := range headers {
			req.Header.Add(key, val)
		}
		err = cli.FastClient.Do(req, resp)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

type rpccc struct {
	rpc.ClientCodec
	ctx context.Context
	thr Throttler
}

func NewRpcClientCodec(cc rpc.ClientCodec, ctx context.Context, thr Throttler) rpccc {
	return rpccc{ClientCodec: cc, ctx: ctx, thr: thr}
}

func (cc rpccc) WriteRequest(req *rpc.Request, body interface{}) (err error) {
	r := NewRunnerSync(cc.ctx, cc.thr)
	r.Run(func(ctx context.Context) error {
		err = cc.ClientCodec.WriteRequest(req, body)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

func (cc rpccc) ReadResponseHeader(resp *rpc.Response) (err error) {
	r := NewRunnerSync(cc.ctx, cc.thr)
	r.Run(func(ctx context.Context) error {
		err = cc.ClientCodec.ReadResponseHeader(resp)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

type rpcsc struct {
	rpc.ServerCodec
	ctx context.Context
	thr Throttler
}

func NewRpcServerCodec(sc rpc.ServerCodec, ctx context.Context, thr Throttler) rpcsc {
	return rpcsc{ServerCodec: sc, ctx: ctx, thr: thr}
}

func (sc rpcsc) ReadRequestHeader(req *rpc.Request) (err error) {
	r := NewRunnerSync(sc.ctx, sc.thr)
	r.Run(func(ctx context.Context) error {
		err = sc.ServerCodec.ReadRequestHeader(req)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

func (sc rpcsc) WriteResponse(resp *rpc.Response, body interface{}) (err error) {
	r := NewRunnerSync(sc.ctx, sc.thr)
	r.Run(func(ctx context.Context) error {
		err = sc.ServerCodec.WriteResponse(resp, body)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

type grpccs struct {
	grpc.ClientStream
	thr Throttler
}

func NewGrpClientStream(cs grpc.ClientStream, thr Throttler) grpccs {
	return grpccs{ClientStream: cs, thr: thr}
}

func (cs grpccs) SendMsg(msg interface{}) (err error) {
	r := NewRunnerSync(cs.Context(), cs.thr)
	r.Run(func(ctx context.Context) error {
		err = cs.ClientStream.SendMsg(msg)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

func (cs grpccs) RecvMsg(msg interface{}) (err error) {
	r := NewRunnerSync(cs.Context(), cs.thr)
	r.Run(func(ctx context.Context) error {
		err = cs.ClientStream.RecvMsg(msg)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

type grpcss struct {
	grpc.ServerStream
	thr Throttler
}

func NewGrpServerStream(ss grpc.ServerStream, thr Throttler) grpcss {
	return grpcss{ServerStream: ss, thr: thr}
}

func (ss grpcss) SetHeader(md metadata.MD) error {
	headers := NewMeta(ss.Context(), ss.thr).Headers()
	for key, val := range headers {
		md.Append(key, val)
	}
	return ss.ServerStream.SetHeader(md)
}

func (ss grpcss) SendMsg(msg interface{}) (err error) {
	r := NewRunnerSync(ss.Context(), ss.thr)
	r.Run(func(ctx context.Context) error {
		err = ss.ServerStream.SendMsg(msg)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

func (ss grpcss) RecvMsg(msg interface{}) (err error) {
	r := NewRunnerSync(ss.Context(), ss.thr)
	r.Run(func(ctx context.Context) error {
		err = ss.ServerStream.RecvMsg(msg)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

type microcli struct {
	client.Client
	thr Throttler
}

func NewMicroClientWrapper(thr Throttler) client.Wrapper {
	return func(cli client.Client) client.Client {
		return microcli{Client: cli, thr: thr}
	}
}

func (cli microcli) Call(ctx context.Context, req client.Request, resp interface{}, opts ...client.CallOption) (err error) {
	r := NewRunnerSync(ctx, cli.thr)
	r.Run(func(ctx context.Context) error {
		err = cli.Client.Call(ctx, req, resp, opts...)
		return nil
	})
	if err := r.Result(); err != nil {
		return err
	}
	return err
}

func NewMicroHandlerWrapper(thr Throttler) server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, resp interface{}) (err error) {
			r := NewRunnerSync(ctx, thr)
			r.Run(func(ctx context.Context) error {
				reqhead := req.Header()
				headers := NewMeta(ctx, thr).Headers()
				for key, val := range headers {
					reqhead[key] = val
				}
				err = h(ctx, req, resp)
				return nil
			})
			if err := r.Result(); err != nil {
				return err
			}
			return err
		}
	}
}
