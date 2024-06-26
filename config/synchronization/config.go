// SPDX-FileCopyrightText: 2024 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0

package synchronization

import "github.com/crossplane/upjet/pkg/config"

const group = "synchronization"

// Configure configures individual resources by adding custom ResourceConfigurators.
func Configure(p *config.Provider) {
	p.AddResourceConfigurator("azuread_synchronization_job", func(r *config.Resource) {
		r.References["service_principal_id"] = config.Reference{
			TerraformName: "azuread_service_principal",
		}
		// We need to override the default group that upjet generated for
		// this resource, which would be "azuread"
		r.ShortGroup = group
	})
	p.AddResourceConfigurator("azuread_synchronization_secret", func(r *config.Resource) {
		r.References["service_principal_id"] = config.Reference{
			TerraformName: "azuread_service_principal",
		}
		// We need to override the default group that upjet generated for
		// this resource, which would be "azuread"
		r.ShortGroup = group
	})
}
