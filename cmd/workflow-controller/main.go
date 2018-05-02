package main

import (
	"fmt"
	"os"
	"time"

	wfclientset "github.com/argoproj/argo/pkg/client/clientset/versioned"
	"github.com/argoproj/argo/pkg/client/clientset/versioned/scheme"
	cmdutil "github.com/argoproj/argo/util/cmd"
	"github.com/argoproj/argo/util/stats"
	"github.com/argoproj/argo/workflow/common"
	"github.com/argoproj/argo/workflow/controller"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	election "k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	// load the gcp plugin (required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// load the oidc plugin (required to authenticate with OpenID Connect).
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// CLIName is the name of the CLI
	CLIName = "workflow-controller"
)

// RootCmd is the argo root level command
var RootCmd = &cobra.Command{
	Use:   CLIName,
	Short: "workflow-controller is the controller to operate on workflows",
	Run:   Run,
}

type rootFlags struct {
	kubeConfig string // --kubeconfig
	configMap  string // --configmap
	logLevel   string // --loglevel
}

var (
	rootArgs rootFlags

	leaseDuration = 15 * time.Second
	renewDuration = 5 * time.Second
	retryPeriod   = 3 * time.Second
)

func init() {
	RootCmd.AddCommand(cmdutil.NewVersionCmd(CLIName))

	RootCmd.Flags().StringVar(&rootArgs.kubeConfig, "kubeconfig", "", "Kubernetes config (used when running outside of cluster)")
	RootCmd.Flags().StringVar(&rootArgs.configMap, "configmap", common.DefaultConfigMapName(common.DefaultControllerDeploymentName), "Name of K8s configmap to retrieve workflow controller configuration")
	RootCmd.Flags().StringVar(&rootArgs.logLevel, "loglevel", "info", "Set the logging level. One of: debug|info|warn|error")
}

// GetClientConfig return rest config, if path not specified, assume in cluster config
func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func Run(cmd *cobra.Command, args []string) {
	cmdutil.SetLogLevel(rootArgs.logLevel)
	stats.RegisterStackDumper()
	stats.StartStatsTicker(5 * time.Minute)

	config, err := GetClientConfig(rootArgs.kubeConfig)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	config.Burst = 30
	config.QPS = 20.0

	kubeclientset := kubernetes.NewForConfigOrDie(config)
	wflientset := wfclientset.NewForConfigOrDie(config)
	leaderElectionClient := kubernetes.NewForConfigOrDie(config)

	// start a controller on instances of our custom resource
	wfController := controller.NewWorkflowController(config, kubeclientset, wflientset, rootArgs.configMap)
	err = wfController.ResyncConfig()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	run := func(stopCh <-chan struct{}) {
		wfController.Run(stopCh, 8, 8)
	}

	id, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	// Prepare event clients.
	eventBroadcaster := record.NewBroadcaster()
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: CLIName})

	namespace := wfController.ConfigMapNS
	rl := &resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      CLIName,
		},
		Client: leaderElectionClient.CoreV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
	}

	election.RunOrDie(election.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDuration,
		RetryPeriod:   retryPeriod,
		Callbacks: election.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				log.Fatalf("leader election lost")
			},
		},
	})

	// Wait forever
	select {}
}
