// SPDX-FileCopyrightText: 2024 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0

package app

import "github.com/crossplane/upjet/pkg/config"

// Configure configures individual resources by adding custom ResourceConfigurators.
func Configure(p *config.Provider) {
	p.AddResourceConfigurator("azuread_app_role_assignment", func(r *config.Resource) {
		r.References["principal_object_id"] = config.Reference{
			TerraformName: "azuread_service_principal",
		}
		r.References["resource_object_id"] = config.Reference{
			TerraformName: "azuread_service_principal",
		}
		// We need to override the default group that upjet generated for
		// this resource, which would be "azuread"
		r.ShortGroup = "app"
	})
}
