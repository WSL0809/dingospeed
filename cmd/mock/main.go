package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"

	manager "dingospeed/pkg/proto/manager"
)

// ---- In-memory data models ----

type File struct {
	Data []byte
	ETag string
}

type Commit struct {
	Sha   string
	Files map[string]*File // path -> file
}

type Repo struct {
	Refs    map[string]string  // ref -> commit
	Commits map[string]*Commit // sha -> commit
}

var repos = map[string]*Repo{}

func addFile(repoName, ref, commitSha, path, etag string, data []byte) {
	r := repos[repoName]
	if r == nil {
		r = &Repo{Refs: map[string]string{}, Commits: map[string]*Commit{}}
		repos[repoName] = r
	}
	c := r.Commits[commitSha]
	if c == nil {
		c = &Commit{Sha: commitSha, Files: map[string]*File{}}
		r.Commits[commitSha] = c
	}
	c.Files[path] = &File{Data: data, ETag: etag}
	r.Refs[ref] = commitSha
}

// ---- Helpers ----

func sliceByRange(data []byte, rng string) (start, end int, body []byte) {
	if rng == "" {
		return 0, len(data) - 1, data
	}
	parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
	fmt.Sscan(parts[0], &start)
	if len(parts) > 1 && parts[1] != "" {
		fmt.Sscan(parts[1], &end)
	} else {
		end = len(data) - 1
	}
	if start < 0 {
		start = 0
	}
	if end >= len(data) {
		end = len(data) - 1
	}
	return start, end, data[start : end+1]
}

// ---- HTTP handlers ----

func revision(c echo.Context) error {
	org := c.Param("org")
	repoName := c.Param("repo")
	repo := org + "/" + repoName
	commit := c.Param("commit")
	r := repos[repo]
	if r == nil || r.Commits[commit] == nil {
		return c.NoContent(http.StatusNotFound)
	}
	if c.Request().Method == http.MethodHead {
		return c.NoContent(http.StatusOK)
	}
	return c.JSON(http.StatusOK, map[string]string{"sha": commit})
}

type pathsReq struct{ Paths []string }

func pathsInfo(c echo.Context) error {
	org := c.Param("org")
	repoName := c.Param("repo")
	repo := org + "/" + repoName
	commit := c.Param("commit")
	r := repos[repo]
	if r == nil {
		return c.NoContent(http.StatusNotFound)
	}
	commitData := r.Commits[commit]
	if commitData == nil {
		return c.NoContent(http.StatusNotFound)
	}
	var req pathsReq
	if err := c.Bind(&req); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	var resp []map[string]any
	for _, p := range req.Paths {
		if f, ok := commitData.Files[p]; ok {
			resp = append(resp, map[string]any{
				"type": "file",
				"oid":  f.ETag,
				"size": len(f.Data),
				"path": p,
			})
		}
	}
	return c.JSON(http.StatusOK, resp)
}

func resolve(c echo.Context) error {
	org := c.Param("org")
	repoName := c.Param("repo")
	repo := org + "/" + repoName
	commit := c.Param("commit")
	path := c.Param("*")
	r := repos[repo]
	if r == nil {
		return c.NoContent(http.StatusNotFound)
	}
	commitData := r.Commits[commit]
	if commitData == nil {
		return c.NoContent(http.StatusNotFound)
	}
	f := commitData.Files[path]
	if f == nil {
		return c.NoContent(http.StatusNotFound)
	}
	start, end, body := sliceByRange(f.Data, c.Request().Header.Get("Range"))
	status := http.StatusOK
	if start != 0 || end != len(f.Data)-1 {
		status = http.StatusPartialContent
		c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.Data)))
	}
	c.Response().Header().Set("Content-Length", fmt.Sprint(len(body)))
	c.Response().Header().Set("ETag", f.ETag)
	c.Response().Header().Set("X-Repo-Commit", commit)
	if c.Request().Method == http.MethodHead {
		return c.NoContent(status)
	}
	c.Response().WriteHeader(status)
	_, err := io.Copy(c.Response().Writer, bytes.NewReader(body))
	return err
}

func refs(c echo.Context) error {
	org := c.Param("org")
	repoName := c.Param("repo")
	repo := org + "/" + repoName
	r := repos[repo]
	if r == nil {
		return c.NoContent(http.StatusNotFound)
	}
	return c.JSON(http.StatusOK, r.Refs)
}

func whoami(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"name": "mock-user", "type": "user"})
}

func wecom(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]int{"errcode": 0})
}

func google(c echo.Context) error {
	if c.Request().Method == http.MethodHead {
		return c.NoContent(http.StatusOK)
	}
	return c.String(http.StatusOK, "OK")
}

// ---- gRPC scheduler mock ----

type schedulerServer struct {
	manager.UnimplementedManagerServer
}

func (s *schedulerServer) Register(ctx context.Context, req *manager.RegisterRequest) (*manager.RegisterResponse, error) {
	return &manager.RegisterResponse{Id: 1, Success: true}, nil
}

func (s *schedulerServer) Heartbeat(ctx context.Context, req *manager.HeartbeatRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *schedulerServer) SchedulerFile(ctx context.Context, req *manager.SchedulerFileRequest) (*manager.SchedulerFileResponse, error) {
	return &manager.SchedulerFileResponse{SchedulerType: 0, ProcessId: 1, Host: "", Port: 0, MasterInstanceId: "", MaxOffset: req.EndPos}, nil
}

func (s *schedulerServer) SyncFileProcess(ctx context.Context, req *manager.SchedulerFileRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *schedulerServer) ReportFileProcess(ctx context.Context, req *manager.FileProcessRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *schedulerServer) DeleteByEtagsAndFields(ctx context.Context, req *manager.DeleteByEtagsAndFieldsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// ---- main ----

func main() {
	// preload demo data
	addFile("openai-community/gpt2", "main", "1234abcd", "config.json", "etag-config", []byte(`{"layer":1}`))
	addFile("openai-community/gpt2", "main", "1234abcd", "pytorch_model.bin", "etag-model", []byte("FAKEBIN"))

	// start gRPC server
	grpcSrv := grpc.NewServer()
	manager.RegisterManagerServer(grpcSrv, &schedulerServer{})
	reflection.Register(grpcSrv)
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc server failed: %v", err)
		}
	}()

	// start HTTP server
	e := echo.New()
	e.GET("/api/:repoType/:org/:repo/revision/:commit", revision)
	e.HEAD("/api/:repoType/:org/:repo/revision/:commit", revision)
	e.POST("/api/:repoType/:org/:repo/paths-info/:commit", pathsInfo)
	e.GET("/:org/:repo/resolve/:commit/*", resolve)
	e.HEAD("/:org/:repo/resolve/:commit/*", resolve)
	e.GET("/:repoType/:org/:repo/resolve/:commit/*", resolve)
	e.HEAD("/:repoType/:org/:repo/resolve/:commit/*", resolve)
	e.GET("/api/:repoType/:org/:repo/refs", refs)
	e.GET("/api/whoami-v2", whoami)
	e.POST("/wecom", wecom)
	e.GET("/google", google)
	e.HEAD("/google", google)

	if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server failed: %v", err)
	}

	grpcSrv.GracefulStop()
}
