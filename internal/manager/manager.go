package manager

import (
	"context"
	"fmt"
	"net"
	"time"

	krmv1alpha1 "github.com/chirino/kube-rsync-machine/api/v1alpha1"
	"github.com/chirino/kube-rsync-machine/internal/control"
	"github.com/chirino/kube-rsync-machine/internal/controlgrpc"
	"github.com/chirino/kube-rsync-machine/internal/controller"
	"github.com/chirino/kube-rsync-machine/internal/liveapi"
	ptmmetrics "github.com/chirino/kube-rsync-machine/internal/metrics"
	"github.com/chirino/kube-rsync-machine/internal/snapshot"
	"github.com/chirino/kube-rsync-machine/internal/tlsutil"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	crmanager "sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type Options struct {
	MetricsBindAddress     string
	HealthProbeBindAddress string
	LeaderElect            bool
	LeaderElectionID       string
	Image                  string
	LiveAPIAddress         string
	FrontendDir            string
	ControlGRPCAddress     string
	Namespace              string
	ControlGRPCTLSSecret   string
	ControlGRPCTLSTTL      time.Duration
}

func Run(ctx context.Context, opts Options) error {
	if opts.Namespace == "" {
		opts.Namespace = DefaultManagerNamespace()
	}
	if opts.ControlGRPCTLSSecret == "" {
		opts.ControlGRPCTLSSecret = DefaultControlGRPCTLSSecretName
	}
	if opts.ControlGRPCTLSTTL == 0 {
		opts.ControlGRPCTLSTTL = DefaultControlGRPCTLSCertificateTTL
	}
	if opts.ControlGRPCAddress == "" {
		return fmt.Errorf("control grpc bind address is required")
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(krmv1alpha1.AddToScheme(scheme))

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{})))

	config := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: opts.MetricsBindAddress,
		},
		HealthProbeBindAddress: opts.HealthProbeBindAddress,
		LeaderElection:         opts.LeaderElect,
		LeaderElectionID:       opts.LeaderElectionID,
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	directClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create direct client: %w", err)
	}
	dataPlaneImage, err := ResolveOwnPodImage(ctx, directClient, opts.Namespace, OwnPodName(), defaultManagerContainerName, opts.Image)
	if err != nil {
		return err
	}
	metricsRecorder := ptmmetrics.NewRecorder()
	if err := metricsRecorder.Register(crmetrics.Registry); err != nil {
		return fmt.Errorf("register metrics: %w", err)
	}
	controlService := control.NewService(control.NewEventHub(256))
	controlGRPCBundle, err := EnsureControlGRPCTLSSecret(ctx, directClient, opts.Namespace, opts.ControlGRPCTLSSecret, opts.ControlGRPCTLSTTL)
	if err != nil {
		return fmt.Errorf("ensure control grpc tls secret: %w", err)
	}
	controlGRPCCA := controlGRPCBundle.CACertPEM
	controlGRPCSigner, err := tlsutil.CAFromPEM(controlGRPCBundle.CACertPEM, controlGRPCBundle.CAKeyPEM)
	if err != nil {
		return fmt.Errorf("load control grpc signer: %w", err)
	}
	controlGRPCCreds, err := controlgrpc.ServerCredentialsWithClientVerifier(controlGRPCBundle, NewRunClientCertificateVerifier(controlGRPCCA))
	if err != nil {
		return fmt.Errorf("build control grpc tls credentials: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("create discovery client: %w", err)
	}
	snapshotCapabilities := snapshot.DiscoveryCapabilities{Client: discoveryClient}
	if err := (&controller.RsyncMachineReconciler{Image: dataPlaneImage}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RsyncMachine controller: %w", err)
	}
	if err := (&controller.BackupSourceReconciler{}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup BackupSource controller: %w", err)
	}
	controlOptions := controller.DefaultDataPlaneControlOptions(opts.Namespace)
	if err := (&controller.BackupJobReconciler{Image: dataPlaneImage, ControlGRPCNamespace: controlOptions.GRPCNamespace, ControlGRPCEndpoint: controlOptions.GRPCEndpoint, Metrics: metricsRecorder, Control: controlService, SnapshotCapabilities: snapshotCapabilities, ControlGRPCCA: controlGRPCCA, ControlGRPCSigner: controlGRPCSigner}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup BackupJob controller: %w", err)
	}
	if err := (&controller.RestoreJobReconciler{Image: dataPlaneImage, ControlGRPCNamespace: controlOptions.GRPCNamespace, ControlGRPCEndpoint: controlOptions.GRPCEndpoint, Metrics: metricsRecorder, ControlGRPCCA: controlGRPCCA, ControlGRPCSigner: controlGRPCSigner}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RestoreJob controller: %w", err)
	}
	if err := mgr.Add(&ControlEventApplier{Client: mgr.GetClient(), Hub: controlService.Hub()}); err != nil {
		return fmt.Errorf("add control event applier: %w", err)
	}
	if opts.LiveAPIAddress != "" {
		api := liveapi.NewWithControl(controlService, liveapi.WithClient(mgr.GetClient()), liveapi.WithFrontendDir(opts.FrontendDir))
		if err := mgr.Add(crmanager.RunnableFunc(func(ctx context.Context) error {
			return liveapi.Serve(ctx, opts.LiveAPIAddress, api.Handler())
		})); err != nil {
			return fmt.Errorf("add live api server: %w", err)
		}
	}
	if err := mgr.Add(crmanager.RunnableFunc(func(ctx context.Context) error {
		listener, err := net.Listen("tcp", opts.ControlGRPCAddress)
		if err != nil {
			return fmt.Errorf("listen control grpc: %w", err)
		}
		server := grpc.NewServer(grpc.Creds(controlGRPCCreds))
		controlgrpc.Register(server, controlService)
		go func() {
			<-ctx.Done()
			server.GracefulStop()
		}()
		if err := server.Serve(listener); err != nil {
			return fmt.Errorf("serve control grpc: %w", err)
		}
		return nil
	})); err != nil {
		return fmt.Errorf("add control grpc server: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add ready check: %w", err)
	}
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start manager: %w", err)
	}
	return nil
}
