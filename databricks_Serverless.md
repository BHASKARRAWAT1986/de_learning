# Decision Record:Databricks Private Network Gateway (PNG) on Azure for Serverless Private Connectivity

# 🧭 Context and Problem Statement

Enterprise customers running **Databricks serverless workloads on Azure** need secure access to resources reachable through their Azure virtual networks, including:

- private APIs and databases hosted on Azure VMs
- on-premises systems connected through ExpressRoute or VPN
- SaaS applications that require dedicated egress paths
- customer-managed firewalls or security appliances for inspected outbound traffic

The Databricks **Private Network Gateway (PNG)** feature addresses this requirement by establishing a tunnel between Databricks serverless compute and a **delegated subnet** in a customer Azure VNet. Instead of configuring a separate private connection for each target resource, customers can delegate one subnet to Databricks and allow serverless workloads to reach destinations permitted through that network path.

PNG is managed through a **Network Connectivity Configuration (NCC)**, which is an account-level object for serverless networking. Once a PNG is created in an NCC and the NCC is attached to one or more workspaces in the same region, supported serverless products in those workspaces can use the gateway automatically.

This decision is needed to document the intended architecture, scope, usage boundaries, and operating considerations for adopting PNG during its Azure Private Preview phase.

# 🎯 Decision Drivers

- Enable private connectivity from Databricks serverless workloads into customer Azure VNets
- Reduce the operational overhead of configuring separate private connectivity for each destination
- Support access to private VNet-hosted services and on-premises resources through existing enterprise network paths
- Support customer-controlled outbound routing through security appliances and dedicated egress paths
- Standardize serverless networking at the Databricks account level using NCC
- Document preview limitations, operational constraints, and routing behavior clearly before adoption
- Ensure DNS, subnet delegation, and workspace attachment requirements are understood and consistently applied
- Clarify the supported scope of PNG, especially for Azure-managed services and service-endpoint-driven traffic

# ✅ Decision Outcome

We will adopt **Databricks Private Network Gateway (PNG)** on Azure as the documented connectivity mechanism for supported **Databricks serverless private networking use cases** during Private Preview, subject to preview limitations and regional support.

PNG will be used through a **Network Connectivity Configuration (NCC)** and attached to eligible Azure Databricks workspaces in the same region.

The documented supported use cases include:

- access to private APIs and databases hosted on VMs inside an Azure VNet
- connectivity to on-premises data sources through existing ExpressRoute or VPN connectivity
- routing serverless egress through customer-managed firewalls or security appliances
- providing dedicated public egress for SaaS access through customer-owned network paths

The following usage constraints apply:

- PNG is currently configured and managed through **REST API only**
- PNG currently supports **serverless Databricks Runtime products**
- Each NCC can contain a maximum of **2 Private Network Gateways**
- PNG requires a **dedicated delegated subnet**, recommended at **/28 or larger**
- The delegated subnet must be in the **same Azure region** as the NCC and attached workspaces
- The delegated subnet subscription and Databricks workspace subscription must be in the **same Azure AD tenant**
- PNG connects to resources in customer virtual networks and does **not** connect to cloud-hosted services such as **ADLS** that use service endpoints
- Traffic precedence rules still apply, including **Private Endpoint** and **Azure Service Endpoint** priority over PNG where applicable
- Blob storage is explicitly excluded from PNG override behavior

**Rationale:**  
The Databricks PNG model provides a scalable way to extend serverless network reach into customer-controlled Azure networking without requiring one private connection per destination. It is intended for enterprise use cases involving VNet resources, hybrid connectivity, and controlled egress paths. The NCC-based architecture also supports centralized reuse across workspaces in the same region.

At the same time, the preview documentation makes the support boundary clear: PNG is designed for resources reachable through the customer VNet and is not a universal routing layer for all Azure-managed services. DNS configuration, route priority, subnet delegation, and workspace attachment are essential parts of the architecture and must be treated as mandatory prerequisites.

# 💰 Cost Implications

According to the Databricks PNG Private Preview documentation:

- There is **no cost associated with using Private Network Gateway during the Private Preview**
- **Billing will be introduced in a future release**

## Upfront costs

Although the feature itself has no preview charge, adoption involves implementation effort.

## Cost uncertainty

Future direct pricing is not yet published. 

# 🛡️ Threat Considerations

**Affected assets and trust boundaries:**

This decision affects the following assets and trust boundaries:

- Databricks serverless compute and its outbound network path
- customer Azure VNets and delegated PNG subnets
- private DNS resolvers and Private DNS Zones
- VNet-hosted private databases and internal APIs
- on-premises resources reached through ExpressRoute or VPN
- customer-managed firewalls, NAT, and security appliances
- routing boundaries between Databricks-managed serverless infrastructure and customer-managed Azure network controls

**Risk assessment:**

| Risk | Category | Mitigation | Validation | Status |
| --- | --- | --- | --- | --- |
| PNG remains in Private Preview with no formal support or SLA | Availability / Vendor | Restrict usage to preview-appropriate environments and maintain close coordination with the Databricks account team | Environment approval review and preview-readiness checkpoints | Accepted |
| Gateway remains in `CREATING` state due to missing subnet delegation or insufficient IP space | Availability / Operational | Ensure subnet is delegated to `Microsoft.Databricks/workspaces` and sized at /28 or larger | Deployment checklist and post-creation state verification | Mitigated |
| DNS resolution fails because the configured resolver is unreachable or the Private DNS Zone is not linked to the correct VNet | Availability / Security | Use reachable DNS resolvers, validate zone linkage, and confirm resolver access from the delegated subnet | DNS resolution tests and environment connectivity validation | Mitigated |
| DNS resolves correctly but traffic times out because NSGs or network rules block the path | Availability / Operational | Validate NSG and routing rules on both PNG subnet and destination subnet | End-to-end connectivity testing and network validation | Mitigated |
| Database connectivity fails even after DNS resolution because the database firewall or auth policy blocks PNG traffic | Security / Operational | Allow PNG subnet source range where required and verify database-side access controls | Application connection tests and database firewall review | Mitigated |
| PNG is established but workloads still cannot connect because the destination DNS name is not configured or the NCC is not attached to the correct workspace | Operational | Validate destination entries and confirm NCC-to-workspace binding before workload rollout | Configuration review and workload certification checklist | Mitigated |
| Traffic to Azure-managed services does not follow PNG because Private Endpoint or Service Endpoint routing takes precedence | Architectural / Operational | Document routing priority and verify whether destination suffixes match Azure-native routes | Routing validation and service-specific connectivity testing | Mitigated |
| Blob storage traffic is incorrectly expected to use PNG | Architectural / Operational | Explicitly document that blob storage is excluded from PNG override behavior | Storage workload design review | Mitigated |
| Traffic is silently dropped because SEG policy is evaluated before PNG and denies the destination | Security / Operational | Check and align SEG allow-lists for overlapping workspaces | Policy validation and workload testing | Mitigated |
| Shared NCC usage across multiple workspaces can increase blast radius of misconfiguration | Security / Operational | Apply controlled change management and validate workspace bindings carefully | Workspace attachment review and staged rollout process | Mitigated |

# 📈 Evaluation Summary

Private Network Gateway introduces a centralized and scalable way to extend **Databricks serverless networking into customer Azure networks**. It is especially well aligned to workloads that must reach:

- VNet-hosted private data sources
- on-premises systems reachable through existing enterprise connectivity
- customer-managed egress controls such as firewalls and dedicated NAT

The solution is governed through **NCC**, making it reusable across multiple workspaces in the same region and simplifying network management at the account level.

At the same time, the preview documentation defines important operational and architectural boundaries:

- PNG is **API-only** during preview
- support is limited to **specific products and regions**
- it depends on correct **subnet delegation**, **DNS**, and **workspace attachment**
- traffic precedence still favors **Private Endpoint** and **Azure Service Endpoint** where applicable
- **cloud-hosted services such as ADLS** are outside PNG’s supported resource scope
- **blob storage traffic cannot be overridden by PNG**

Overall, PNG provides a strong enterprise connectivity mechanism for supported private networking use cases, with adoption dependent on clear operational guardrails and careful validation of routing and DNS behavior.

# 🌐 Links

- Databricks Account Console: `accounts.azuredatabricks.net`
- NCC create API: `POST /api/2.0/accounts/{accountId}/network-connectivity-configs`
- PNG create API: `POST /api/2.0/accounts/{accountId}/network-connectivity-configs/{networkConnectivityConfigId}/private-network-gateways`
- PNG get API: `GET /api/2.0/accounts/{accountId}/network-connectivity-configs/{networkConnectivityConfigId}/private-network-gateways/{gatewayId}`
- Workspace attach API: `PATCH /api/2.0/accounts/{accountId}/workspaces/{workspaceId}`
- PNG update API: `PATCH /api/2.0/accounts/{accountId}/network-connectivity-configs/{networkConnectivityConfigId}/private-network-gateways/{gatewayId}?update_mask=destinations,traffic_mode`
- PNG delete API: `DELETE /api/2.0/accounts/{accountId}/network-connectivity-configs/{networkConnectivityConfigId}/private-network-gateways/{gatewayId}`
- PNG list API: `GET /api/2.0/accounts/{accountId}/network-connectivity-configs/{networkConnectivityConfigId}/private-network-gateways`
