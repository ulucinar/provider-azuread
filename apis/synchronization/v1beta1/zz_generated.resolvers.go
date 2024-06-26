// SPDX-FileCopyrightText: 2024 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0
// Code generated by angryjet. DO NOT EDIT.
// Code transformed by upjet. DO NOT EDIT.

package v1beta1

import (
	"context"
	reference "github.com/crossplane/crossplane-runtime/pkg/reference"
	errors "github.com/pkg/errors"

	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	client "sigs.k8s.io/controller-runtime/pkg/client"

	// ResolveReferences of this Job.
	apisresolver "github.com/upbound/provider-azuread/internal/apis"
)

func (mg *Job) ResolveReferences(ctx context.Context, c client.Reader) error {
	var m xpresource.Managed
	var l xpresource.ManagedList
	r := reference.NewAPIResolver(c, mg)

	var rsp reference.ResolutionResponse
	var err error
	{
		m, l, err = apisresolver.GetManagedResource("serviceprincipals.azuread.upbound.io", "v1beta2", "Principal", "PrincipalList")
		if err != nil {
			return errors.Wrap(err, "failed to get the reference target managed resource and its list for reference resolution")
		}

		rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
			CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.ServicePrincipalID),
			Extract:      reference.ExternalName(),
			Reference:    mg.Spec.ForProvider.ServicePrincipalIDRef,
			Selector:     mg.Spec.ForProvider.ServicePrincipalIDSelector,
			To:           reference.To{List: l, Managed: m},
		})
	}
	if err != nil {
		return errors.Wrap(err, "mg.Spec.ForProvider.ServicePrincipalID")
	}
	mg.Spec.ForProvider.ServicePrincipalID = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.ServicePrincipalIDRef = rsp.ResolvedReference
	{
		m, l, err = apisresolver.GetManagedResource("serviceprincipals.azuread.upbound.io", "v1beta2", "Principal", "PrincipalList")
		if err != nil {
			return errors.Wrap(err, "failed to get the reference target managed resource and its list for reference resolution")
		}

		rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
			CurrentValue: reference.FromPtrValue(mg.Spec.InitProvider.ServicePrincipalID),
			Extract:      reference.ExternalName(),
			Reference:    mg.Spec.InitProvider.ServicePrincipalIDRef,
			Selector:     mg.Spec.InitProvider.ServicePrincipalIDSelector,
			To:           reference.To{List: l, Managed: m},
		})
	}
	if err != nil {
		return errors.Wrap(err, "mg.Spec.InitProvider.ServicePrincipalID")
	}
	mg.Spec.InitProvider.ServicePrincipalID = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.InitProvider.ServicePrincipalIDRef = rsp.ResolvedReference

	return nil
}

// ResolveReferences of this Secret.
func (mg *Secret) ResolveReferences(ctx context.Context, c client.Reader) error {
	var m xpresource.Managed
	var l xpresource.ManagedList
	r := reference.NewAPIResolver(c, mg)

	var rsp reference.ResolutionResponse
	var err error
	{
		m, l, err = apisresolver.GetManagedResource("serviceprincipals.azuread.upbound.io", "v1beta2", "Principal", "PrincipalList")
		if err != nil {
			return errors.Wrap(err, "failed to get the reference target managed resource and its list for reference resolution")
		}

		rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
			CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.ServicePrincipalID),
			Extract:      reference.ExternalName(),
			Reference:    mg.Spec.ForProvider.ServicePrincipalIDRef,
			Selector:     mg.Spec.ForProvider.ServicePrincipalIDSelector,
			To:           reference.To{List: l, Managed: m},
		})
	}
	if err != nil {
		return errors.Wrap(err, "mg.Spec.ForProvider.ServicePrincipalID")
	}
	mg.Spec.ForProvider.ServicePrincipalID = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.ServicePrincipalIDRef = rsp.ResolvedReference
	{
		m, l, err = apisresolver.GetManagedResource("serviceprincipals.azuread.upbound.io", "v1beta2", "Principal", "PrincipalList")
		if err != nil {
			return errors.Wrap(err, "failed to get the reference target managed resource and its list for reference resolution")
		}

		rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
			CurrentValue: reference.FromPtrValue(mg.Spec.InitProvider.ServicePrincipalID),
			Extract:      reference.ExternalName(),
			Reference:    mg.Spec.InitProvider.ServicePrincipalIDRef,
			Selector:     mg.Spec.InitProvider.ServicePrincipalIDSelector,
			To:           reference.To{List: l, Managed: m},
		})
	}
	if err != nil {
		return errors.Wrap(err, "mg.Spec.InitProvider.ServicePrincipalID")
	}
	mg.Spec.InitProvider.ServicePrincipalID = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.InitProvider.ServicePrincipalIDRef = rsp.ResolvedReference

	return nil
}
