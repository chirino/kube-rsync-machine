package liveapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Server struct {
	Hub         *control.EventHub
	Control     *control.Service
	Client      client.Client
	FrontendDir string
}

type Option func(*Server)

func WithClient(c client.Client) Option {
	return func(s *Server) {
		s.Client = c
	}
}

func WithFrontendDir(dir string) Option {
	return func(s *Server) {
		s.FrontendDir = dir
	}
}

func New(hub *control.EventHub, opts ...Option) *Server {
	if hub == nil {
		hub = control.NewEventHub(128)
	}
	return NewWithControl(control.NewService(hub), opts...)
}

func NewWithControl(service *control.Service, opts ...Option) *Server {
	if service == nil {
		service = control.NewService(control.NewEventHub(128))
	}
	server := &Server{Hub: service.Hub(), Control: service}
	for _, opt := range opts {
		opt(server)
	}
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/namespaces/", s.handleNamespaced)
	mux.HandleFunc("/api/v1/events", s.handleGlobalEvents)
	mux.HandleFunc("/api/v1/machines", s.handleGlobalList)
	mux.HandleFunc("/api/v1/backups", s.handleGlobalList)
	mux.HandleFunc("/api/v1/restores", s.handleGlobalList)
	mux.HandleFunc("/api/v1/sources", s.handleGlobalList)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	})
	if s.FrontendDir != "" {
		mux.Handle("/", spaFileHandler(s.FrontendDir))
	}
	return mux
}

func (s *Server) handleGlobalEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	s.writeFilteredEvents(w, r)
}

func (s *Server) handleNamespaced(w http.ResponseWriter, r *http.Request) {
	route, ok := parseNamespacedPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	switch route.resource {
	case "backups":
		if route.stream {
			s.writeEvents(w, r, control.RunKey{Namespace: route.namespace, Name: route.name, Kind: control.RunKindBackup})
			return
		}
		if route.name == "" {
			s.writeBackupJobs(w, r, route.namespace)
			return
		}
		s.writeBackupJob(w, r, route.namespace, route.name)
	case "restores":
		if route.stream {
			s.writeEvents(w, r, control.RunKey{Namespace: route.namespace, Name: route.name, Kind: control.RunKindRestore})
			return
		}
		if route.name == "" {
			s.writeRestoreJobs(w, r, route.namespace)
			return
		}
		s.writeRestoreJob(w, r, route.namespace, route.name)
	case "machines":
		if route.restorePoints {
			s.writeRestorePoints(w, r, route.namespace, route.name)
			return
		}
		if route.name == "" {
			s.writeMachines(w, r, route.namespace)
			return
		}
		s.writeMachine(w, r, route.namespace, route.name)
	case "sources":
		if route.name == "" {
			s.writeSources(w, r, route.namespace)
			return
		}
		s.writeSource(w, r, route.namespace, route.name)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (s *Server) handleGlobalList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	switch strings.TrimPrefix(r.URL.Path, "/api/v1/") {
	case "machines":
		s.writeMachines(w, r, namespace)
	case "backups":
		s.writeBackupJobs(w, r, namespace)
	case "restores":
		s.writeRestoreJobs(w, r, namespace)
	case "sources":
		s.writeSources(w, r, namespace)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (s *Server) writeSnapshot(w http.ResponseWriter, _ *http.Request, key control.RunKey) {
	events, err := s.Hub.Snapshot(key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) writeEvents(w http.ResponseWriter, r *http.Request, key control.RunKey) {
	afterSequence, err := parseLastEventID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_last_event_id", err.Error())
		return
	}
	events, err := s.Hub.SubscribeAfter(r.Context(), key, afterSequence)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	s.writeEventStream(w, r, events, nil)
}

func (s *Server) writeFilteredEvents(w http.ResponseWriter, r *http.Request) {
	afterSequence, err := parseLastEventID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_last_event_id", err.Error())
		return
	}
	filter, err := parseEventFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_filter", err.Error())
		return
	}
	events, err := s.Hub.SubscribeAllAfter(r.Context(), afterSequence)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	s.writeEventStream(w, r, events, filter.matches)
}

func (s *Server) writeEventStream(w http.ResponseWriter, r *http.Request, events <-chan control.ControlEvent, include func(control.ControlEvent) bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			if include != nil && !include(event) {
				continue
			}
			payload, err := json.Marshal(event)
			if err != nil {
				fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
				flusher.Flush()
				continue
			}
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Kind, payload)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) writeMachines(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.ensureClient(w) {
		return
	}
	var machines krmv1alpha1.RsyncMachineList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := s.Client.List(r.Context(), &machines, opts...); err != nil {
		writeClientError(w, err)
		return
	}
	var sources krmv1alpha1.BackupSourceList
	if err := s.Client.List(r.Context(), &sources); err != nil {
		writeClientError(w, err)
		return
	}
	query := r.URL.Query()
	machines.Items = filterMachines(machines.Items, sources.Items, namespace, query.Get("source"))
	writeJSON(w, http.StatusOK, machines)
}

func (s *Server) writeMachine(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.ensureClient(w) {
		return
	}
	var machine krmv1alpha1.RsyncMachine
	if err := s.Client.Get(r.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &machine); err != nil {
		writeClientError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, machine)
}

func (s *Server) writeBackupJobs(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.ensureClient(w) {
		return
	}
	var runs krmv1alpha1.BackupJobList
	if err := s.Client.List(r.Context(), &runs, listOptions(namespace)...); err != nil {
		writeClientError(w, err)
		return
	}
	query := r.URL.Query()
	runs.Items = filterBackupJobs(runs.Items, namespace, query.Get("phase"), query.Get("machine"))
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) writeBackupJob(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if s.Client == nil {
		s.writeSnapshot(w, r, control.RunKey{Namespace: namespace, Name: name, Kind: control.RunKindBackup})
		return
	}
	var run krmv1alpha1.BackupJob
	if err := s.Client.Get(r.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &run); err != nil {
		writeClientError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) writeRestoreJobs(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.ensureClient(w) {
		return
	}
	var runs krmv1alpha1.RestoreJobList
	if err := s.Client.List(r.Context(), &runs, listOptions(namespace)...); err != nil {
		writeClientError(w, err)
		return
	}
	var sources krmv1alpha1.BackupSourceList
	if err := s.Client.List(r.Context(), &sources); err != nil {
		writeClientError(w, err)
		return
	}
	query := r.URL.Query()
	runs.Items = filterRestoreJobs(runs.Items, sources.Items, namespace, query.Get("phase"), query.Get("machine"), query.Get("source"))
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) writeRestoreJob(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if s.Client == nil {
		s.writeSnapshot(w, r, control.RunKey{Namespace: namespace, Name: name, Kind: control.RunKindRestore})
		return
	}
	var run krmv1alpha1.RestoreJob
	if err := s.Client.Get(r.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &run); err != nil {
		writeClientError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) writeRestorePoints(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.ensureClient(w) {
		return
	}
	var target krmv1alpha1.RsyncMachine
	if err := s.Client.Get(r.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &target); err != nil {
		writeClientError(w, err)
		return
	}
	points := filterRestorePoints(target.Status.RestorePoints, r.URL.Query().Get("tier"), r.URL.Query().Get("snapshot"))
	writeJSON(w, http.StatusOK, restorePointList{Items: points})
}

func (s *Server) writeSources(w http.ResponseWriter, r *http.Request, namespace string) {
	if !s.ensureClient(w) {
		return
	}
	var sources krmv1alpha1.BackupSourceList
	if err := s.Client.List(r.Context(), &sources, listOptions(namespace)...); err != nil {
		writeClientError(w, err)
		return
	}
	query := r.URL.Query()
	sources.Items = filterSources(sources.Items, query.Get("pvc"), query.Get("capture"))
	writeJSON(w, http.StatusOK, sources)
}

func (s *Server) writeSource(w http.ResponseWriter, r *http.Request, namespace, name string) {
	if !s.ensureClient(w) {
		return
	}
	var source krmv1alpha1.BackupSource
	if err := s.Client.Get(r.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &source); err != nil {
		writeClientError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, source)
}

func (s *Server) ensureClient(w http.ResponseWriter) bool {
	if s.Client != nil {
		return true
	}
	writeError(w, http.StatusNotImplemented, "cache_unavailable", "cached Kubernetes listing is not wired")
	return false
}

func listOptions(namespace string) []client.ListOption {
	if namespace == "" {
		return nil
	}
	return []client.ListOption{client.InNamespace(namespace)}
}

type eventFilter struct {
	namespace string
	kinds     map[string]struct{}
}

func parseEventFilter(r *http.Request) (eventFilter, error) {
	query := r.URL.Query()
	filter := eventFilter{namespace: strings.TrimSpace(query.Get("namespace"))}
	if filter.namespace != "" && !validNamespace(filter.namespace) {
		return eventFilter{}, fmt.Errorf("namespace must be a valid DNS label")
	}
	for _, kind := range query["kind"] {
		kind = strings.TrimSpace(kind)
		if kind == "" {
			continue
		}
		switch kind {
		case control.RunKindBackup, control.RunKindRestore:
			if filter.kinds == nil {
				filter.kinds = map[string]struct{}{}
			}
			filter.kinds[kind] = struct{}{}
		default:
			return eventFilter{}, fmt.Errorf("unsupported run kind %q", kind)
		}
	}
	return filter, nil
}

func (f eventFilter) matches(event control.ControlEvent) bool {
	if f.namespace != "" && event.Key.Namespace != f.namespace {
		return false
	}
	if len(f.kinds) > 0 {
		if _, ok := f.kinds[event.Key.Kind]; !ok {
			return false
		}
	}
	return true
}

func writeJSONStream[T any](w http.ResponseWriter, r *http.Request, values <-chan T) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	encoder := json.NewEncoder(w)
	for {
		select {
		case value, ok := <-values:
			if !ok {
				return
			}
			if err := encoder.Encode(value); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

type namespacedRoute struct {
	namespace     string
	resource      string
	name          string
	stream        bool
	restorePoints bool
}

func parseNamespacedPath(path string) (namespacedRoute, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "namespaces" {
		return namespacedRoute{}, false
	}
	route := namespacedRoute{namespace: parts[3], resource: parts[4]}
	if !validNamespace(route.namespace) {
		return namespacedRoute{}, false
	}
	switch route.resource {
	case "backups", "restores", "machines", "sources":
	default:
		return namespacedRoute{}, false
	}
	if len(parts) == 5 {
		return route, true
	}
	if len(parts) == 6 && validObjectName(parts[5]) {
		route.name = parts[5]
		return route, true
	}
	if len(parts) == 7 && validObjectName(parts[5]) {
		route.name = parts[5]
		if (route.resource == "backups" || route.resource == "restores") && parts[6] == "events" {
			route.stream = true
			return route, true
		}
		if route.resource == "machines" && (parts[6] == "restorepoints" || parts[6] == "restore-points") {
			route.restorePoints = true
			return route, true
		}
	}
	return namespacedRoute{}, false
}

type restorePointList struct {
	Items []krmv1alpha1.RestorePoint `json:"items"`
}

type errorResponse struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: apiError{Code: code, Message: message}})
}

func writeMethodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeClientError(w http.ResponseWriter, err error) {
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	writeError(w, http.StatusInternalServerError, "kubernetes_error", err.Error())
}

func spaFileHandler(root string) http.Handler {
	cleanRoot := filepath.Clean(root)
	indexPath := filepath.Join(cleanRoot, "index.html")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeMethodNotAllowed(w, http.MethodGet, http.MethodHead)
			return
		}
		requestPath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))
		if requestPath == "/" {
			requestPath = "/index.html"
		}
		filePath := filepath.Join(cleanRoot, filepath.FromSlash(strings.TrimPrefix(requestPath, "/")))
		if !pathInside(cleanRoot, filePath) {
			writeError(w, http.StatusBadRequest, "invalid_path", "static asset path escapes frontend root")
			return
		}
		if info, err := os.Stat(filePath); err == nil && !info.IsDir() {
			http.ServeFile(w, r, filePath)
			return
		}
		http.ServeFile(w, r, indexPath)
	})
}

func pathInside(root, filePath string) bool {
	rel, err := filepath.Rel(root, filePath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func parseLastEventID(r *http.Request) (uint64, error) {
	value := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if value == "" {
		value = strings.TrimSpace(r.URL.Query().Get("lastEventId"))
	}
	if value == "" {
		return 0, nil
	}
	sequence, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("last event id must be an unsigned integer")
	}
	return sequence, nil
}

func validNamespace(value string) bool {
	return value != "" && len(validation.IsDNS1123Label(value)) == 0
}

func validObjectName(value string) bool {
	return value != "" && len(validation.IsDNS1123Subdomain(value)) == 0
}

func filterMachines(machines []krmv1alpha1.RsyncMachine, sources []krmv1alpha1.BackupSource, defaultNamespace, sourceFilter string) []krmv1alpha1.RsyncMachine {
	if sourceFilter == "" {
		return machines
	}
	out := make([]krmv1alpha1.RsyncMachine, 0, len(machines))
	for _, machine := range machines {
		if defaultNamespace != "" && machine.Namespace != defaultNamespace {
			continue
		}
		if !machineHasSource(machine, sources, sourceFilter) {
			continue
		}
		out = append(out, machine)
	}
	return out
}

func machineHasSource(machine krmv1alpha1.RsyncMachine, sources []krmv1alpha1.BackupSource, sourceFilter string) bool {
	machineRef := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	for _, source := range sources {
		if source.Spec.MachineRef.Name == "" {
			continue
		}
		sourceMachineRef := types.NamespacedName{
			Namespace: source.Spec.MachineRef.NamespaceOr(source.Namespace),
			Name:      source.Spec.MachineRef.Name,
		}
		if sourceMachineRef != machineRef {
			continue
		}
		if refMatches(machine.Namespace, krmv1alpha1.ObjectReference{Namespace: source.Namespace, Name: source.Name}, sourceFilter) {
			return true
		}
	}
	return false
}

func filterBackupJobs(runs []krmv1alpha1.BackupJob, namespace, phaseFilter, machineFilter string) []krmv1alpha1.BackupJob {
	if phaseFilter == "" && machineFilter == "" {
		return runs
	}
	out := make([]krmv1alpha1.BackupJob, 0, len(runs))
	for _, run := range runs {
		if phaseFilter != "" && string(run.Status.Phase) != phaseFilter {
			continue
		}
		if machineFilter != "" && !backupJobMatchesMachine(run, namespace, machineFilter) {
			continue
		}
		out = append(out, run)
	}
	return out
}

func backupJobMatchesMachine(run krmv1alpha1.BackupJob, namespace, machineFilter string) bool {
	if refMatches(namespace, run.Spec.MachineRef, machineFilter) {
		return true
	}
	return anyRefMatches(namespace, run.Status.IncludedMachines, machineFilter)
}

func filterRestoreJobs(runs []krmv1alpha1.RestoreJob, sources []krmv1alpha1.BackupSource, namespace, phaseFilter, machineFilter, sourceFilter string) []krmv1alpha1.RestoreJob {
	if phaseFilter == "" && machineFilter == "" && sourceFilter == "" {
		return runs
	}
	sourcesByRef := map[types.NamespacedName]krmv1alpha1.BackupSource{}
	for _, source := range sources {
		sourcesByRef[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}] = source
	}
	out := make([]krmv1alpha1.RestoreJob, 0, len(runs))
	for _, run := range runs {
		if phaseFilter != "" && string(run.Status.Phase) != phaseFilter {
			continue
		}
		if sourceFilter != "" && !refMatches(namespace, run.Spec.SourceRef, sourceFilter) {
			continue
		}
		if machineFilter != "" {
			sourceRef := namespacedObjectReference(run.Spec.SourceRef, run.Namespace)
			source, ok := sourcesByRef[sourceRef]
			if !ok || !refMatches(namespace, source.Spec.MachineRef, machineFilter) {
				continue
			}
		}
		out = append(out, run)
	}
	return out
}

func namespacedObjectReference(ref krmv1alpha1.ObjectReference, defaultNamespace string) types.NamespacedName {
	return types.NamespacedName{Namespace: ref.NamespaceOr(defaultNamespace), Name: ref.Name}
}

func filterSources(sources []krmv1alpha1.BackupSource, pvcFilter, captureFilter string) []krmv1alpha1.BackupSource {
	if pvcFilter == "" && captureFilter == "" {
		return sources
	}
	out := make([]krmv1alpha1.BackupSource, 0, len(sources))
	for _, source := range sources {
		if pvcFilter != "" && source.Spec.PVC != pvcFilter {
			continue
		}
		if captureFilter != "" && string(source.Spec.Consistency.CaptureOrDefault()) != captureFilter {
			continue
		}
		out = append(out, source)
	}
	return out
}

func filterRestorePoints(points []krmv1alpha1.RestorePoint, tierFilter, snapshotFilter string) []krmv1alpha1.RestorePoint {
	if tierFilter == "" && snapshotFilter == "" {
		return points
	}
	out := make([]krmv1alpha1.RestorePoint, 0, len(points))
	for _, point := range points {
		if tierFilter != "" && point.Tier != tierFilter {
			continue
		}
		if snapshotFilter != "" && point.Snapshot != snapshotFilter {
			continue
		}
		out = append(out, point)
	}
	return out
}

func refMatches(defaultNamespace string, ref krmv1alpha1.ObjectReference, filter string) bool {
	namespace, name := splitNamespacedFilter(defaultNamespace, filter)
	return ref.NamespaceOr(defaultNamespace) == namespace && ref.Name == name
}

func anyRefMatches(defaultNamespace string, refs []krmv1alpha1.ObjectReference, filter string) bool {
	for _, ref := range refs {
		if refMatches(defaultNamespace, ref, filter) {
			return true
		}
	}
	return false
}

func splitNamespacedFilter(defaultNamespace, filter string) (string, string) {
	if before, after, ok := strings.Cut(filter, "/"); ok {
		return before, after
	}
	return defaultNamespace, filter
}

func Serve(ctx context.Context, addr string, handler http.Handler) error {
	if addr == "" {
		return nil
	}
	server := &http.Server{Addr: addr, Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
