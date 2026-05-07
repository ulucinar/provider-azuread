// HO.

package ratelimiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/upbound/provider-azuread/v2/internal/clients"
)

const (
	errGetManaged               = "cannot get managed resource"
)

type efrlConfig struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

// EncapsulatingRateLimiter
//
// TODO: we need to synchronize access to the embedded maps here (use synchronized map?)
type EncapsulatingRateLimiter struct {
	inner map[reconcile.Request]workqueue.TypedRateLimiter[reconcile.Request]
	delayConfigs map[types.UID]efrlConfig
}

func (c *EncapsulatingRateLimiter) When(item reconcile.Request) time.Duration {
	when := c.inner[item].When(item)
	fmt.Println("---------------------------------------------------------------")
	fmt.Println("Item: " + item.String())
	fmt.Println("When: " + when.String())
	fmt.Println("Num requeues: " + strconv.Itoa(c.inner[item].NumRequeues(item)))
	fmt.Println("---------------------------------------------------------------")

	return when
}

func (c *EncapsulatingRateLimiter) Forget(item reconcile.Request) {
	// already removed, nothing to forget
	if _, ok := c.inner[item]; !ok {
		return
	}
	c.inner[item].Forget(item)
}

func (c *EncapsulatingRateLimiter) NumRequeues(item reconcile.Request) int {
	return c.inner[item].NumRequeues(item)
}

func NewEncapsulatingRateLimiter() *EncapsulatingRateLimiter {
	return &EncapsulatingRateLimiter{
		inner: make(map[reconcile.Request]workqueue.TypedRateLimiter[reconcile.Request]),
		delayConfigs: make(map[types.UID]efrlConfig),
	}
}

type Configurations struct {
	RateLimiter *EncapsulatingRateLimiter
}

type ConfigurableReconciler struct {
	inner reconcile.Reconciler
	manager manager.Manager
	gvk schema.GroupVersionKind
	configs Configurations
}

// Reconcile the supplied request subject to rate limiting.
func (r *ConfigurableReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	mg := resource.MustCreateObject(r.gvk, r.manager.GetScheme()).(resource.Managed)

	if err := r.manager.GetClient().Get(ctx, req.NamespacedName, mg); err != nil {
		// There's no need to requeue if we no longer exist. Otherwise we'll be
		// requeued implicitly because we return an error.
		// log.Debug("Cannot get managed resource", "error", err)
		return reconcile.Result{}, errors.Wrap(resource.IgnoreNotFound(err), errGetManaged)
	}

	uid := mg.GetUID()

	pcSpec, err := clients.ResolveProviderConfig(ctx, r.manager.GetClient(), mg)
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, "cannot resolve the referenced ProviderConfig")
	}

	if _, ok := r.configs.RateLimiter.delayConfigs[uid]; !ok {
		r.configs.RateLimiter.delayConfigs[uid] = efrlConfig{
			baseDelay: 1*time.Second,
			maxDelay:  60*time.Second,
		}
		r.configs.RateLimiter.inner[req] = workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](1*time.Second, 60*time.Second)
	}

	if pcSpec.ReconciliationPolicy != nil && pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter != nil {
		if r.configs.RateLimiter.delayConfigs[uid].baseDelay != pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.BaseDelay.Duration ||
			r.configs.RateLimiter.delayConfigs[uid].maxDelay != pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.MaxDelay.Duration {
			r.configs.RateLimiter.inner[req] = workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
				pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.BaseDelay.Duration,
				pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.MaxDelay.Duration)
			r.configs.RateLimiter.delayConfigs[uid] = efrlConfig{
				baseDelay: pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.BaseDelay.Duration,
				maxDelay:  pcSpec.ReconciliationPolicy.ExponentialFailureRateLimiter.MaxDelay.Duration,
			}
		}
	}

	return r.inner.Reconcile(ctx, req)
}

// NewConfigurableReconciler wraps the supplied Reconciler.
func NewConfigurableReconciler(m manager.Manager, r reconcile.Reconciler, gvk schema.GroupVersionKind, configs Configurations) *ConfigurableReconciler {
	return &ConfigurableReconciler{
		manager: m,
		inner: r,
		gvk: gvk,
		configs: configs,
	}
}

func NewConfigurationFinalizer(inner resource.Finalizer, configs Configurations) *ConfigurationFinalizer {
	return &ConfigurationFinalizer{
		Finalizer: inner,
		configs:   configs,
	}
}

type ConfigurationFinalizer struct {
	resource.Finalizer
	configs Configurations
}

// AddFinalizer to the supplied Managed resource.
func (cf *ConfigurationFinalizer) AddFinalizer(ctx context.Context, obj resource.Object) error {
	return cf.Finalizer.AddFinalizer(ctx, obj)
}

// RemoveFinalizer removes the workspace from workspace store before removing
// the finalizer.
func (cf *ConfigurationFinalizer) RemoveFinalizer(ctx context.Context, obj resource.Object) error {
	delete(cf.configs.RateLimiter.delayConfigs, obj.GetUID())
	delete(cf.configs.RateLimiter.inner, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
		},
	})

	return cf.Finalizer.RemoveFinalizer(ctx, obj)
}