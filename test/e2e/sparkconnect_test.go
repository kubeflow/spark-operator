package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("SparkConnect Controller", func() {
	Context("Reconcile a new SparkConnect", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file", func() {
				file, err := os.Open(path)
				Expect(err).NotTo(HaveOccurred())
				Expect(file).NotTo(BeNil())
				defer func() {
					Expect(file.Close()).To(Succeed())
				}()

				decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
				Expect(decoder).NotTo(BeNil())

				conn = &v1alpha1.SparkConnect{}
				Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
			})

			By("Creating SparkConnect", func() {
				Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			})
		})

		AfterEach(func() {
			By("Deleting SparkConnect", func() {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
				conn = nil
			})
		})

		It("Should reconcile SparkConnect correctly", func() {
			By("Waiting for server pod to be ready", func() {
				Eventually(func() bool {
					key := types.NamespacedName{
						Namespace: conn.Namespace,
						Name:      sparkconnect.GetServerPodName(conn),
					}
					server := &corev1.Pod{}
					if err := k8sClient.Get(ctx, key, server); err != nil {
						return false
					}
					return util.IsPodReady(server)
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})

			By("Waiting for executor pods to be ready", func() {
				Eventually(func() bool {
					executors := &corev1.PodList{}
					Expect(k8sClient.List(
						ctx,
						executors,
						client.InNamespace(conn.Namespace),
						client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn))),
					).NotTo(HaveOccurred())

					instances := int(ptr.Deref(conn.Spec.Executor.Instances, 0))
					if len(executors.Items) != instances {
						return false
					}

					for _, executor := range executors.Items {
						if !util.IsPodReady(&executor) {
							return false
						}
					}
					return true
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})

			By("Waiting for server service to be created", func() {
				Eventually(func() bool {
					key := types.NamespacedName{
						Namespace: conn.Namespace,
						Name:      sparkconnect.GetServerServiceName(conn),
					}
					service := &corev1.Service{}
					if err := k8sClient.Get(ctx, key, service); err != nil {
						return false
					}
					return true
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})
		})
	})

	Context("Restart policy", func() {
		ctx := context.Background()

		var conn *v1alpha1.SparkConnect
		var created bool

		BeforeEach(func() {
			created = false
		})

		AfterEach(func() {
			if created {
				By("Deleting SparkConnect", func() {
					Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, conn))).To(Succeed())
					conn = nil
				})
			}

		})

		parseSparkConnect := func(path string) {
			By("Parsing SparkConnect from file", func() {
				file, err := os.Open(path)
				Expect(err).NotTo(HaveOccurred())
				Expect(file).NotTo(BeNil())
				defer func() {
					Expect(file.Close()).To(Succeed())
				}()

				decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
				Expect(decoder).NotTo(BeNil())

				conn = &v1alpha1.SparkConnect{}
				Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
			})
		}

		createSparkConnect := func() {
			By("Creating SparkConnect", func() {
				Expect(k8sClient.Create(ctx, conn)).To(Succeed())
				created = true
			})
		}

		expectFailingServerEnv := func() {
			Expect(conn.Spec.Server.Template).NotTo(BeNil())
			Expect(conn.Spec.Server.Template.Spec.Containers).NotTo(BeEmpty())

			container := conn.Spec.Server.Template.Spec.Containers[0]
			Expect(container.Command).To(BeEmpty())
			Expect(container.Args).To(BeEmpty())
			Expect(container.Env).To(ContainElement(corev1.EnvVar{
				Name:  "SPARK_HOME",
				Value: "/non-existent-spark-home",
			}))
		}

		sparkConnectKey := func() types.NamespacedName {
			return types.NamespacedName{
				Namespace: conn.Namespace,
				Name:      conn.Name,
			}
		}

		serverPodKey := func() types.NamespacedName {
			return types.NamespacedName{
				Namespace: conn.Namespace,
				Name:      sparkconnect.GetServerPodName(conn),
			}
		}

		waitForServerPodPhase := func(phase corev1.PodPhase, waitTimeout time.Duration) corev1.Pod {
			var server corev1.Pod

			Eventually(func() corev1.PodPhase {
				if err := k8sClient.Get(ctx, serverPodKey(), &server); err != nil {
					return ""
				}
				return server.Status.Phase
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(Equal(phase))

			return server
		}

		waitForSparkConnectState := func(state v1alpha1.SparkConnectState, waitTimeout time.Duration) v1alpha1.SparkConnect {
			var latest v1alpha1.SparkConnect

			Eventually(func(g Gomega) v1alpha1.SparkConnectState {
				if err := k8sClient.Get(ctx, sparkConnectKey(), &latest); err != nil {
					return ""
				}
				if state != v1alpha1.SparkConnectStateFailed {
					g.Expect(latest.Status.State).NotTo(Equal(v1alpha1.SparkConnectStateFailed))
				}
				return latest.Status.State
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(Equal(state))

			return latest
		}

		waitForRestartAttempt := func(attempt int32, waitTimeout time.Duration) v1alpha1.SparkConnect {
			var latest v1alpha1.SparkConnect

			Eventually(func(g Gomega) int32 {
				if err := k8sClient.Get(ctx, sparkConnectKey(), &latest); err != nil {
					return -1
				}
				g.Expect(latest.Status.State).NotTo(Equal(v1alpha1.SparkConnectStateFailed))
				return latest.Status.RestartAttempts
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(Equal(attempt))

			return latest
		}

		waitForGenerationAfter := func(generation int64, waitTimeout time.Duration) v1alpha1.SparkConnect {
			var latest v1alpha1.SparkConnect

			Eventually(func() int64 {
				if err := k8sClient.Get(ctx, sparkConnectKey(), &latest); err != nil {
					return 0
				}
				return latest.Generation
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(BeNumerically(">", generation))

			return latest
		}

		waitForObservedGenerationAndRestartAttempt := func(generation int64, attempt int32, waitTimeout time.Duration) v1alpha1.SparkConnect {
			var latest v1alpha1.SparkConnect

			Eventually(func() bool {
				if err := k8sClient.Get(ctx, sparkConnectKey(), &latest); err != nil {
					return false
				}
				return latest.Status.ObservedGeneration == generation && latest.Status.RestartAttempts == attempt
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(BeTrue())

			return latest
		}

		startRestartableFailedStateMonitor := func(maxRestartAttempts int32) (func(), <-chan v1alpha1.SparkConnect) {
			monitorCtx, cancel := context.WithCancel(ctx)
			failedState := make(chan v1alpha1.SparkConnect, 1)
			done := make(chan struct{})

			go func() {
				defer close(done)

				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				for {
					latest := &v1alpha1.SparkConnect{}
					if err := k8sClient.Get(monitorCtx, sparkConnectKey(), latest); err == nil {
						if latest.Status.RestartAttempts < maxRestartAttempts &&
							latest.Status.State == v1alpha1.SparkConnectStateFailed {
							failedState <- *latest.DeepCopy()
							return
						}
					}

					select {
					case <-monitorCtx.Done():
						return
					case <-ticker.C:
					}
				}
			}()

			stop := func() {
				cancel()
				<-done
			}

			return stop, failedState
		}

		expectNoRestartableFailedState := func(failedState <-chan v1alpha1.SparkConnect, maxRestartAttempts int32) {
			select {
			case latest := <-failedState:
				Fail(fmt.Sprintf(
					"SparkConnect state became Failed before restart attempts were exhausted: restartAttempts=%d, maxRestartAttempts=%d, observedGeneration=%d, generation=%d",
					latest.Status.RestartAttempts,
					maxRestartAttempts,
					latest.Status.ObservedGeneration,
					latest.Generation,
				))
			default:
			}
		}

		waitForNextServerPodUID := func(seen map[types.UID]struct{}, waitTimeout time.Duration) types.UID {
			var uid types.UID

			Eventually(func(g Gomega) types.UID {
				latest := &v1alpha1.SparkConnect{}
				if err := k8sClient.Get(ctx, sparkConnectKey(), latest); err == nil {
					g.Expect(latest.Status.State).NotTo(Equal(v1alpha1.SparkConnectStateFailed))
				}

				server := &corev1.Pod{}
				if err := k8sClient.Get(ctx, serverPodKey(), server); err != nil {
					return ""
				}
				if server.UID == "" {
					return ""
				}
				if _, ok := seen[server.UID]; ok {
					return ""
				}
				uid = server.UID
				return uid
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).ShouldNot(BeEmpty())

			seen[uid] = struct{}{}
			return uid
		}

		expectNoAdditionalServerPods := func(uids []types.UID, waitTimeout time.Duration) {
			seen := map[types.UID]struct{}{}
			for _, uid := range uids {
				seen[uid] = struct{}{}
			}

			Consistently(func() int {
				server := &corev1.Pod{}
				if err := k8sClient.Get(ctx, serverPodKey(), server); err != nil {
					return len(seen)
				}
				if server.UID != "" {
					seen[server.UID] = struct{}{}
				}
				return len(seen)
			}).WithPolling(PollInterval).WithTimeout(waitTimeout).Should(Equal(len(uids)))
		}

		It("Should not restart failed server pod when restart policy is Never", func() {
			parseSparkConnect(filepath.Join("bad_examples", "fail-sparkconnect-restart-never.yaml"))
			Expect(conn.Spec.RestartConfig.RestartPolicy).To(Equal(v1alpha1.SparkConnectRestartPolicyNever))
			expectFailingServerEnv()
			createSparkConnect()

			server := waitForServerPodPhase(corev1.PodFailed, WaitTimeout)

			finalConn := waitForSparkConnectState(v1alpha1.SparkConnectStateFailed, WaitTimeout)
			Expect(finalConn.Status.RestartAttempts).To(Equal(int32(0)))

			Consistently(func() types.UID {
				latest := &corev1.Pod{}
				if err := k8sClient.Get(ctx, serverPodKey(), latest); err != nil {
					return ""
				}
				return latest.UID
			}).WithPolling(PollInterval).WithTimeout(PollInterval * 3).Should(Equal(server.UID))
		})

		It("Should restart failed server pod up to max restart attempts when restart policy is OnFailure", func() {
			parseSparkConnect(filepath.Join("bad_examples", "fail-sparkconnect-restart.yaml"))
			Expect(conn.Spec.RestartConfig.RestartPolicy).To(Equal(v1alpha1.SparkConnectRestartPolicyOnFailure))
			Expect(conn.Spec.RestartConfig.MaxRestartAttempts).NotTo(BeNil())
			Expect(conn.Spec.RestartConfig.RestartBackoffMillis).NotTo(BeNil())
			restartBackoffMillis := *conn.Spec.RestartConfig.RestartBackoffMillis
			restartAttemptTimeout := time.Duration(restartBackoffMillis)*time.Millisecond + 10*time.Second

			expectFailingServerEnv()
			createSparkConnect()

			server := waitForServerPodPhase(corev1.PodFailed, WaitTimeout)
			Expect(server.UID).NotTo(BeEmpty())

			uids := []types.UID{server.UID}
			seen := map[types.UID]struct{}{
				server.UID: {},
			}
			stopFailedStateMonitor, failedState := startRestartableFailedStateMonitor(*conn.Spec.RestartConfig.MaxRestartAttempts)
			defer stopFailedStateMonitor()
			for attempt := int32(1); attempt <= *conn.Spec.RestartConfig.MaxRestartAttempts; attempt++ {
				waitForRestartAttempt(attempt, restartAttemptTimeout)
				waitForSparkConnectState(v1alpha1.SparkConnectStateNotReady, 10*time.Second)
				uids = append(uids, waitForNextServerPodUID(seen, restartAttemptTimeout))
			}
			stopFailedStateMonitor()
			expectNoRestartableFailedState(failedState, *conn.Spec.RestartConfig.MaxRestartAttempts)
			Expect(uids).To(HaveLen(int(*conn.Spec.RestartConfig.MaxRestartAttempts) + 1))

			waitForSparkConnectState(v1alpha1.SparkConnectStateFailed, WaitTimeout)
			expectNoAdditionalServerPods(uids, PollInterval*5)
		})

		It("Should reset restart attempts after observing a new generation", func() {
			parseSparkConnect(filepath.Join("bad_examples", "fail-sparkconnect-restart.yaml"))
			Expect(conn.Spec.RestartConfig.RestartPolicy).To(Equal(v1alpha1.SparkConnectRestartPolicyOnFailure))
			Expect(conn.Spec.RestartConfig.MaxRestartAttempts).NotTo(BeNil())
			Expect(conn.Spec.RestartConfig.RestartBackoffMillis).NotTo(BeNil())
			restartBackoffMillis := *conn.Spec.RestartConfig.RestartBackoffMillis
			restartAttemptTimeout := time.Duration(restartBackoffMillis)*time.Millisecond + 10*time.Second

			expectFailingServerEnv()
			createSparkConnect()

			server := waitForServerPodPhase(corev1.PodFailed, WaitTimeout)
			Expect(server.UID).NotTo(BeEmpty())
			seen := map[types.UID]struct{}{
				server.UID: {},
			}

			firstRestartConn := waitForRestartAttempt(1, restartAttemptTimeout)
			waitForNextServerPodUID(seen, restartAttemptTimeout)
			waitForServerPodPhase(corev1.PodFailed, WaitTimeout)

			latest := &v1alpha1.SparkConnect{}
			Expect(k8sClient.Get(ctx, sparkConnectKey(), latest)).To(Succeed())
			previousGeneration := firstRestartConn.Generation
			original := latest.DeepCopy()
			latest.Spec.RestartConfig.RestartPolicy = v1alpha1.SparkConnectRestartPolicyNever
			Expect(k8sClient.Patch(ctx, latest, client.MergeFrom(original))).To(Succeed())

			updatedConn := waitForGenerationAfter(previousGeneration, WaitTimeout)
			resetConn := waitForObservedGenerationAndRestartAttempt(updatedConn.Generation, 0, WaitTimeout)
			Expect(resetConn.Status.State).To(Equal(v1alpha1.SparkConnectStateFailed))
		})

		It("Should restart failed server pod up to max restart attempts when restart policy is Always", func() {
			parseSparkConnect(filepath.Join("bad_examples", "fail-sparkconnect-restart-always.yaml"))
			Expect(conn.Spec.RestartConfig.RestartPolicy).To(Equal(v1alpha1.SparkConnectRestartPolicyAlways))
			Expect(conn.Spec.RestartConfig.MaxRestartAttempts).NotTo(BeNil())
			Expect(conn.Spec.RestartConfig.RestartBackoffMillis).NotTo(BeNil())
			restartBackoffMillis := *conn.Spec.RestartConfig.RestartBackoffMillis
			restartAttemptTimeout := time.Duration(restartBackoffMillis)*time.Millisecond + 10*time.Second

			expectFailingServerEnv()
			createSparkConnect()

			server := waitForServerPodPhase(corev1.PodFailed, WaitTimeout)
			Expect(server.UID).NotTo(BeEmpty())

			uids := []types.UID{server.UID}
			seen := map[types.UID]struct{}{
				server.UID: {},
			}
			stopFailedStateMonitor, failedState := startRestartableFailedStateMonitor(*conn.Spec.RestartConfig.MaxRestartAttempts)
			defer stopFailedStateMonitor()
			for attempt := int32(1); attempt <= *conn.Spec.RestartConfig.MaxRestartAttempts; attempt++ {
				waitForRestartAttempt(attempt, restartAttemptTimeout)
				waitForSparkConnectState(v1alpha1.SparkConnectStateNotReady, 10*time.Second)
				uids = append(uids, waitForNextServerPodUID(seen, restartAttemptTimeout))
			}
			stopFailedStateMonitor()
			expectNoRestartableFailedState(failedState, *conn.Spec.RestartConfig.MaxRestartAttempts)
			Expect(uids).To(HaveLen(int(*conn.Spec.RestartConfig.MaxRestartAttempts) + 1))

			waitForSparkConnectState(v1alpha1.SparkConnectStateFailed, WaitTimeout)
			expectNoAdditionalServerPods(uids, PollInterval*5)
		})

	})

})
