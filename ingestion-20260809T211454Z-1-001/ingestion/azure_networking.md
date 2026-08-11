# Azure Networking — Complete Deep Dive Guide

> **Goal of this file:** Understand EVERY networking concept in Azure —
> what it is, why it exists, how it connects to other resources,
> and real-world scenarios: resource group to resource group,
> storage access, on-premises to Azure, on-prem to VM/Storage.

---

## Table of Contents

1. [The Mental Model — Azure as a City](#1-the-mental-model--azure-as-a-city)
2. [Virtual Network (VNet)](#2-virtual-network-vnet)
3. [Subnet](#3-subnet)
4. [Network Security Group (NSG)](#4-network-security-group-nsg)
5. [Network Interface Card (NIC)](#5-network-interface-card-nic)
6. [Public IP Address](#6-public-ip-address)
7. [NAT Gateway](#7-nat-gateway)
8. [VNet Peering](#8-vnet-peering)
9. [VNet Gateway (VPN Gateway)](#9-vnet-gateway-vpn-gateway)
10. [ExpressRoute](#10-expressroute)
11. [Service Endpoint](#11-service-endpoint)
12. [Private Endpoint & Private Link](#12-private-endpoint--private-link)
13. [Azure Firewall](#13-azure-firewall)
14. [User Defined Routes (UDR) & Route Tables](#14-user-defined-routes-udr--route-tables)
15. [Application Gateway & Load Balancer](#15-application-gateway--load-balancer)
16. [DNS in Azure](#16-dns-in-azure)
17. [How Resource Groups Fit into Networking](#17-how-resource-groups-fit-into-networking)
18. [Communication Scenarios — All Patterns](#18-communication-scenarios--all-patterns)
19. [Storage Access Scenarios](#19-storage-access-scenarios)
20. [On-Premises Connectivity Scenarios](#20-on-premises-connectivity-scenarios)
21. [Full Architecture Example — Enterprise Setup](#21-full-architecture-example--enterprise-setup)
22. [Decision Trees — Which Networking Tool to Use](#22-decision-trees--which-networking-tool-to-use)
23. [Quick Reference Cheat Sheet](#23-quick-reference-cheat-sheet)

---

## 1. The Mental Model — Azure as a City

Before diving into individual concepts, build a mental model:

```
Azure Region = A CITY (e.g., "East US City")

  VNet = A PRIVATE GATED COMMUNITY inside the city
         Only residents (resources with private IPs) can enter by default.
         The community has its own internal roads (subnets).

  Subnet = A STREET inside the community
           Web servers on Elm Street, databases on Oak Street.
           Streets can have security checkpoints (NSGs).

  Resource Group = AN ADMINISTRATIVE FOLDER
                   It's NOT a network boundary.
                   Resources in the same RG can be in different VNets.
                   Resources in different RGs can share a VNet.

  NSG = A SECURITY GUARD at a street checkpoint
        Checks IDs (IPs/ports) and decides who can enter/leave.

  VNet Peering = A CONNECTING ROAD between two gated communities
                 Residents of Community A can visit Community B directly.

  VPN Gateway = A SECURE TUNNEL under the ocean
                Connects your city (Azure) to another city (on-premises).

  ExpressRoute = A PRIVATE HIGHWAY to your data center
                 Not going over the internet at all. Dedicated line.

  Private Endpoint = THE STORE OPENS A BRANCH INSIDE YOUR COMMUNITY
                     ADLS (which normally lives "in the city center" publicly)
                     opens a private door directly inside your gated community.

  Azure Firewall = A TOLL BOOTH + INSPECTION STATION
                   All traffic leaving/entering the community goes through it.
                   Reads, logs, and can block traffic by rules.

  NAT Gateway = A SHARED EXIT GATE
                When your residents need to go outside (internet),
                they all appear as the same face (shared public IP).
                Outsiders can't figure out which internal resident it was.
```

---

## 2. Virtual Network (VNet)

### What is it?
A VNet is your **private network inside Azure**. It is logically isolated — no one outside can access it unless you explicitly allow it.

```
┌──────────────────────────────────────────────────────────────┐
│  VNet: vnet-prod                                              │
│  Address Space: 10.0.0.0/16                                  │
│  Region: East US                                              │
│  Subscription: sub-prod                                       │
│                                                               │
│  This gives you IPs: 10.0.0.0 → 10.0.255.255                │
│  Total IPs: 65,536                                            │
│                                                               │
│  Resources inside can talk to each other using private IPs.  │
│  Resources outside CANNOT reach inside by default.           │
└──────────────────────────────────────────────────────────────┘
```

### Key facts
```
✅ Scoped to a REGION (cannot span regions — use peering for that)
✅ Scoped to a SUBSCRIPTION
✅ Can have multiple address spaces (e.g., 10.0.0.0/16 + 192.168.0.0/24)
✅ Free — you pay for resources INSIDE it, not the VNet itself
❌ Cannot overlap address spaces with peered VNets
❌ Cannot move a VNet to a different region
```

### Creating a VNet — what to decide
```
Name:          vnet-prod-eastus
Region:        East US            ← ALL resources in this VNet must be in East US
Address space: 10.0.0.0/16       ← Reserve enough IPs for future growth
               Plan: web=10.0.1.0/24, db=10.0.2.0/24, dbr=10.0.3.0/24, spare=...
```

### CIDR refresher
```
/8   → 16,777,216 IPs  (massive — whole company)
/16  → 65,536 IPs      (VNet level — common)
/24  → 256 IPs         (subnet — very common)
/26  → 64 IPs          (small subnet)
/28  → 16 IPs          (tiny — maybe just a gateway)
/29  → 8 IPs           (too small for most uses — 3 usable after Azure reserves 5)

Azure always RESERVES 5 IPs per subnet:
  .0   = network address
  .1   = default gateway
  .2   = Azure DNS
  .3   = Azure DNS  
  .255 = broadcast
  So /29 (8 IPs) → only 3 usable!
```

---

## 3. Subnet

### What is it?
A subnet is a **segment of your VNet's address space**. You deploy resources into subnets, not directly into VNets.

```
VNet: 10.0.0.0/16
├── Subnet: web-tier     10.0.1.0/24   (web servers and APIs)
├── Subnet: app-tier     10.0.2.0/24   (application logic)
├── Subnet: data-tier    10.0.3.0/24   (databases, caches)
├── Subnet: pe-subnet    10.0.4.0/24   (private endpoints only)
├── Subnet: gw-subnet    10.0.5.0/27   (VPN/App Gateway)
└── Subnet: fw-subnet    10.0.0.0/26   (Azure Firewall — must be named AzureFirewallSubnet)
```

### Why separate subnets?
```
Security isolation:
  NSG on web-tier allows port 80/443 from internet.
  NSG on data-tier allows port 5432 ONLY from app-tier.
  → Even if a web server is hacked, attacker can't directly reach the database.

Service delegation:
  Some Azure services require a dedicated subnet:
  - Databricks cluster VMs → Microsoft.Databricks/workspaces
  - Azure NetApp Files → Microsoft.NetApp/volumes
  - Azure Container Instances → Microsoft.ContainerInstance/containerGroups

Performance grouping:
  Put resources that talk to each other frequently in the same subnet
  (or at minimum the same VNet) to minimize latency.
```

### Subnet communication rules (default, no NSG)
```
Same subnet:    Resource A → Resource B     ✅ ALLOWED (always)
Same VNet:      Subnet A   → Subnet B       ✅ ALLOWED (by default)
Different VNet: VNet A     → VNet B         ❌ BLOCKED (need peering)
Internet in:    Internet   → any VM         ❌ BLOCKED (need Public IP + NSG allow)
Internet out:   VM         → Internet       ✅ ALLOWED (outbound by default)
```

---

## 4. Network Security Group (NSG)

### What is it?
An NSG is a **stateful firewall** — a list of allow/deny rules for inbound and outbound traffic.

```
NSG rule anatomy:
┌─────────────────────────────────────────────────────────────────┐
│ Priority │ Source        │ Source Port │ Dest         │ Dest Port │ Action │
├──────────┼───────────────┼─────────────┼──────────────┼───────────┼────────┤
│ 100      │ 203.0.113.5   │ *           │ 10.0.1.4     │ 443       │ Allow  │
│ 200      │ 10.0.2.0/24   │ *           │ 10.0.3.0/24  │ 5432      │ Allow  │
│ 300      │ Internet      │ *           │ 10.0.3.0/24  │ *         │ Deny   │
│ 65500    │ *             │ *           │ *            │ *         │ Deny   │ ← default
└─────────────────────────────────────────────────────────────────┘

Rules evaluated lowest priority number FIRST.
First matching rule wins — rest are ignored.
Default rule 65500: deny all (Azure built-in, cannot delete).
```

### NSG Stateful behavior
```
STATEFUL means:
If you ALLOW inbound traffic on port 443,
Azure automatically allows the RETURN traffic (response) outbound.
You don't need a separate outbound rule for the response.

Example:
  Inbound rule: Allow Internet → VM:443 ✅
  User sends request to your VM on port 443
  VM responds back to user (ephemeral port 54321)
  → This outbound response is automatically allowed (stateful tracking)
  → You do NOT need "Outbound Allow to Internet:54321"
```

### NSG placement — subnet vs NIC
```
You can attach NSG to:
  A SUBNET → applies to ALL resources in that subnet
  A NIC    → applies to only that one VM's network card

Both can coexist — traffic must pass BOTH NSG rule sets.
Best practice: attach to SUBNET (easier to manage, fewer NSGs overall).

         INTERNET
            │
     ┌──────▼──────┐
     │ Subnet NSG  │   ← first check
     │ (subnet A)  │
     └──────┬──────┘
            │
     ┌──────▼──────┐
     │   NIC NSG   │   ← second check
     │  (VM's NIC) │
     └──────┬──────┘
            │
          VM
```

### Service Tags — shortcuts for Azure service IPs
```
Instead of memorizing IP ranges, use service tags:

Service Tag          Covers
────────────────     ────────────────────────────────────────
Internet             All public internet IPs
VirtualNetwork       All IPs in the VNet + peered VNets
AzureLoadBalancer    Azure health probe IPs
AzureCloud           All Azure datacenter IPs
AzureStorage         All Azure Storage public IPs
AzureKeyVault        All Azure Key Vault public IPs
AzureDatabricks      Databricks control plane IPs
Sql                  Azure SQL public IPs

Example NSG rule using service tags:
  Allow: AzureKeyVault (outbound) → port 443
  (no need to maintain a list of Key Vault IPs — Microsoft updates the tag)
```

---

## 5. Network Interface Card (NIC)

### What is it?
A NIC is the **virtual network adapter attached to a VM**. Every VM has at least one NIC. The NIC holds the VM's private IP (and optionally public IP).

```
VM: vm-webserver
└── NIC: vm-webserver-nic1
    ├── Private IP: 10.0.1.4   (from web-tier subnet)
    ├── Public IP:  52.x.x.x   (optional, for direct internet access)
    ├── Subnet:     web-tier
    └── NSG:        nsg-webserver  (optional, in addition to subnet NSG)
```

```
Key points:
- Private IP is assigned from subnet CIDR range
- Private IP can be DYNAMIC (changes on VM restart) or STATIC (fixed)
- ALWAYS use STATIC private IPs for servers, databases, DNS forwarders
- A VM can have MULTIPLE NICs (for network separation — e.g., one NIC on internet-facing subnet, one on internal subnet)
```

---

## 6. Public IP Address

### What is it?
A Public IP is an **internet-routable IP address** that Azure assigns. It's a separate resource you attach to a NIC or Load Balancer.

```
Two SKUs:
  BASIC  → older, being retired, no zone redundancy
  STANDARD → current, zone-redundant, required for most new features

Two assignment types:
  DYNAMIC → IP can change when you stop/deallocate the VM
             (fine for testing, bad for production)
  STATIC  → IP never changes
             (required for DNS records, firewall rules, production)
```

```
Do you ALWAYS need a Public IP on a VM?

  For internet-facing apps:   YES (or use Load Balancer which has 1 public IP)
  For internal-only servers:  NO  — use private IPs only
  For management (SSH/RDP):   Strongly recommend USING BASTION instead of Public IP
  
  Best practice for prod: NO public IPs on VMs at all.
  Use Azure Bastion for admin access.
  Use Load Balancer for app access.
  Use Private Endpoints for service access.
```

---

## 7. NAT Gateway

### What is it?
NAT Gateway provides **outbound internet connectivity** for VMs that have no Public IP (No Public IP / NPIP setup).

### The problem it solves
```
PROBLEM:
  You have VMs with only private IPs (10.0.1.4, 10.0.1.5).
  They have no public IP.
  They need to download packages from PyPI, call external APIs.
  How do they get out to the internet?

WITHOUT NAT Gateway:
  Azure gives each VM a random "default outbound" IP from Azure's shared pool.
  This IP CHANGES unpredictably. External services can't allowlist it.
  Azure is RETIRING default outbound in Sept 2025 anyway.

WITH NAT Gateway:
  You attach a fixed Public IP (or prefix) to the NAT Gateway.
  All outbound traffic from the subnet goes through this NAT Gateway.
  External world sees: 52.x.x.x (your stable, known IP).
  You can allowlist this IP in any external firewall or API.
```

### How NAT Gateway works
```
┌────────────────────────────────────────────────────────────────┐
│ Subnet: 10.0.1.0/24  (NAT Gateway attached)                    │
│                                                                  │
│  VM1: 10.0.1.4  ──┐                                             │
│  VM2: 10.0.1.5  ──┤── outbound traffic → NAT Gateway           │
│  VM3: 10.0.1.6  ──┘         │                                   │
│                              │ translates private IP to         │
│                              │ public IP (SNAT — Source NAT)    │
└──────────────────────────────│─────────────────────────────────┘
                               │
                          Public IP: 52.100.1.1
                               │
                          Internet (PyPI, APIs, etc.)

INBOUND (from internet to NAT'd VMs): ❌ NOT ALLOWED
NAT Gateway is OUTBOUND ONLY.
If you need inbound, use a Load Balancer or Public IP on the VM.
```

### NAT Gateway vs other options
```
┌──────────────────┬───────────────┬─────────────────┬──────────────────┐
│ Method           │ Outbound IP   │ Inbound allowed │ Recommended for  │
├──────────────────┼───────────────┼─────────────────┼──────────────────┤
│ VM Public IP     │ VM's own IP   │ Yes (with NSG)  │ Dev/test only    │
│ Default outbound │ Random Azure  │ No              │ Avoid (retiring) │
│ NAT Gateway      │ Stable/static │ No              │ Production VMs   │
│ Azure Firewall   │ Firewall IP   │ Yes             │ Enterprise/inspect│
│ Load Balancer    │ LB's IP       │ Yes             │ Web apps         │
└──────────────────┴───────────────┴─────────────────┴──────────────────┘
```

---

## 8. VNet Peering

### What is it?
VNet Peering **connects two VNets** so resources in each can communicate using private IPs — as if they were on the same network.

```
Two types:
  Local Peering:   Both VNets in the SAME Azure region
  Global Peering:  VNets in DIFFERENT Azure regions
                   (slightly higher latency, slightly higher cost — per GB)
```

### How peering works
```
BEFORE peering:
  VNet-A: 10.0.0.0/16  (VM-A: 10.0.1.4)
  VNet-B: 10.1.0.0/16  (VM-B: 10.1.1.4)

  VM-A pings 10.1.1.4 → NO ROUTE → Fails ❌

AFTER peering (VNet-A ↔ VNet-B):
  VM-A pings 10.1.1.4 → Azure backbone → VM-B ✅
  Traffic NEVER touches public internet.
  Latency = same as intra-VNet (milliseconds).

┌──────────────────┐       Peering        ┌──────────────────┐
│ VNet-A           │◄────────────────────►│ VNet-B           │
│ 10.0.0.0/16      │   Azure backbone     │ 10.1.0.0/16      │
│                  │   (no internet)      │                  │
│ VM-A: 10.0.1.4   │                      │ VM-B: 10.1.1.4   │
└──────────────────┘                      └──────────────────┘
```

### Peering is NON-TRANSITIVE
```
This is a critical point — often misunderstood.

VNet-A ←→ VNet-B  (peered)
VNet-B ←→ VNet-C  (peered)

Does VNet-A reach VNet-C? ❌ NO — NOT TRANSITIVE.

You must explicitly peer A↔C if needed.

The EXCEPTION: Hub-Spoke with Azure Firewall + UDR
  If VNet-B has a firewall and you set up UDRs:
  Traffic: VNet-A → firewall in VNet-B → VNet-C  ✅ (transit via firewall)
  This is the "Hub-Spoke" architecture pattern.

┌──────┐       ┌──────────────────────┐       ┌──────┐
│VNet-A│◄─────►│ VNet-B (HUB)         │◄─────►│VNet-C│
└──────┘       │  Azure Firewall      │       └──────┘
               │  Route tables (UDR)  │
               └──────────────────────┘
A → C = A → Firewall(B) → C  ✅ (traffic inspected)
```

### Peering rules
```
✅ Works across subscriptions (same Azure AD tenant)
✅ Works across regions (global peering)
❌ Address spaces CANNOT OVERLAP
   VNet-A: 10.0.0.0/16 and VNet-B: 10.0.5.0/24 → OVERLAP → can't peer
❌ Peering is NOT automatic for new subnets — it covers the entire VNet
✅ Peering must be created on BOTH sides (A→B and B→A)
✅ After peering, NSG rules on both VNets still apply
```

### Real example — multi-region peering
```
Company has East US and West US workloads:

East US VNet: vnet-eastus (10.0.0.0/16)
  └── App VMs, databases

West US VNet: vnet-westus (10.1.0.0/16)
  └── DR (disaster recovery) VMs

Global Peering: vnet-eastus ←→ vnet-westus
  East US app VM (10.0.1.4) → reads from West US DR DB (10.1.2.5)
  Traffic goes through Azure backbone, not internet.
  Cost: ~$0.02/GB (global peering data transfer)
```

---

## 9. VNet Gateway (VPN Gateway)

### What is it?
A VPN Gateway **creates an encrypted tunnel (IPsec/IKE)** between your Azure VNet and another network — either on-premises or another Azure VNet.

### Types of VPN Gateway connections

```
┌─────────────────────────────────────────────────────────────────────┐
│                    VPN GATEWAY CONNECTION TYPES                      │
├─────────────────────┬───────────────────────────────────────────────┤
│ Site-to-Site (S2S)  │ Azure VNet ←→ On-Premises network             │
│                     │ Always-on tunnel between networks               │
│                     │ Requires on-prem VPN device (router/firewall)  │
├─────────────────────┼───────────────────────────────────────────────┤
│ Point-to-Site (P2S) │ Azure VNet ←→ Individual device               │
│                     │ Employee laptop connecting to Azure VNet        │
│                     │ No on-prem device needed, just VPN client       │
├─────────────────────┼───────────────────────────────────────────────┤
│ VNet-to-VNet        │ Azure VNet ←→ Another Azure VNet               │
│                     │ Encrypted tunnel between VNets                  │
│                     │ (VNet Peering is usually preferred — simpler)   │
└─────────────────────┴───────────────────────────────────────────────┘
```

### Site-to-Site VPN — How it works
```
Your Office / Data Center                    Azure
────────────────────────                     ──────────────────────────
On-Prem Network: 192.168.0.0/24             VNet: 10.0.0.0/16

On-Prem VPN Device              IPsec/IKE    Azure VPN Gateway
(Cisco, Fortinet, pfSense) ←── encrypted ──► (in GatewaySubnet)
192.168.0.1                       tunnel      10.0.5.4

After tunnel is up:
  On-prem VM (192.168.0.10) → 10.0.1.5 (Azure VM) ✅ via encrypted tunnel
  Azure VM (10.0.1.5) → 192.168.0.10 (on-prem)   ✅ via encrypted tunnel
  No traffic touches public internet unencrypted.

Important requirements:
  1. A dedicated "GatewaySubnet" subnet in your VNet (must be named exactly "GatewaySubnet")
     Minimum /27 (but /26 recommended)
  2. A Public IP for the Azure VPN Gateway
  3. A "Local Network Gateway" object in Azure — defines on-prem network range + on-prem VPN device's public IP
  4. VPN device on-prem must support IKEv1/IKEv2
  5. Gateway SKU: Basic (dev), VpnGw1, VpnGw2... (throughput: 650Mbps → 10Gbps)
```

### Point-to-Site VPN — Developer connecting to Azure
```
Scenario: Developer needs to SSH into an internal Azure VM that has no Public IP.

SETUP:
  1. Create VPN Gateway (with P2S config)
  2. Choose auth method: Azure certificate or Azure AD (recommended)
  3. Define client address pool: 172.16.0.0/24 (IPs for VPN clients)
  4. Developer installs Azure VPN Client on laptop
  5. Downloads VPN profile from Azure

RESULT:
  Developer laptop (home IP: 100.x.x.x)
  ↓  opens VPN connection
  Gets assigned: 172.16.0.5 (from client pool)
  ↓  encrypted tunnel to Azure VPN Gateway
  Can now SSH to: 10.0.1.5 (Azure VM private IP)  ✅
  
  Other colleagues can also connect — each gets a different IP from the pool.
  Revoke access by removing their certificate or Azure AD account.
```

### VPN Gateway vs VNet Peering
```
┌──────────────────────┬──────────────────────┬──────────────────────┐
│ Aspect               │ VNet Peering          │ VPN Gateway (V2V)    │
├──────────────────────┼──────────────────────┼──────────────────────┤
│ Latency              │ Very low              │ Higher (encrypt/decrypt)│
│ Bandwidth            │ VNet limit (high)     │ Gateway SKU limit    │
│ Cost                 │ Data transfer per GB  │ Gateway hourly + data│
│ Setup complexity     │ Low                   │ Higher               │
│ Cross-subscription   │ Yes                   │ Yes                  │
│ Cross-tenant         │ Yes (with perms)      │ Yes                  │
│ Encryption           │ No (Azure backbone)   │ Yes (IPsec)          │
│ Use case             │ Azure-to-Azure        │ Azure + on-prem,     │
│                      │ private routing       │ encrypted required   │
└──────────────────────┴──────────────────────┴──────────────────────┘
```

---

## 10. ExpressRoute

### What is it?
ExpressRoute is a **private, dedicated physical connection** between your on-premises network and Azure — no internet involved at all.

```
INTERNET VPN (VPN Gateway):        EXPRESSROUTE:
  Your DC ─── internet ─── Azure    Your DC ─── carrier/partner ─── Azure
  Encrypted but goes over internet  Physical fiber, never touches internet
  Up to 1.25 Gbps (VpnGw5)         10 Gbps, 100 Gbps options
  Variable latency                  Consistent low latency (SLA)
  ~$30-200/month gateway cost       Expensive: $500–$10,000+/month
  Good for: most companies          Good for: banks, hospitals, large enterprises
```

### ExpressRoute circuit
```
Your On-Prem Data Center
        │
        │ (Fiber from your DC to nearest ISP PoP)
        ▼
  ISP / Carrier Partner  (e.g., AT&T, Equinix, Megaport)
        │
        │ (Dedicated private circuit — ExpressRoute circuit)
        ▼
  Microsoft Enterprise Edge (MSEE) Routers
        │
        │ (Azure backbone)
        ▼
  Your Azure VNets (via Virtual Network Gateway with ExpressRoute type)
```

### ExpressRoute vs VPN for on-prem connectivity
```
Use VPN Gateway (IPsec) when:
  ✅ Budget is limited
  ✅ Variable bandwidth is OK
  ✅ Non-critical workloads
  ✅ Remote offices, developer access

Use ExpressRoute when:
  ✅ Compliance requires no internet path (banking, healthcare, government)
  ✅ Consistent low latency required
  ✅ Moving LARGE amounts of data (data migration, backup)
  ✅ SLA for connectivity is required
```

---

## 11. Service Endpoint

### What is it?
A Service Endpoint extends your VNet's identity to Azure PaaS services (like ADLS, SQL, Key Vault) — traffic stays on Azure backbone but the service still has a public IP.

```
WITHOUT Service Endpoint:
  VM (10.0.1.4) → request to ADLS
  → traffic exits VNet → goes to ADLS public IP (20.150.x.x)
  → goes over public Azure backbone (or even internet)
  → ADLS has no idea which VNet sent the request

WITH Service Endpoint:
  Enable Service Endpoint on the subnet for "Microsoft.Storage"
  VM (10.0.1.4) → request to ADLS
  → traffic stays on Azure backbone (optimized route, not internet)
  → arrives at ADLS public IP (20.150.x.x) BUT with VNet identity header
  → ADLS firewall: "Allow from vnet-prod subnet" ✅
  → ADLS: serves the request

What changes:
  ✅ Traffic stays on Azure backbone (not internet)
  ✅ ADLS firewall can restrict to "only from this specific subnet"
  ✅ Simple to enable (one checkbox in subnet settings)
  ❌ ADLS still has a PUBLIC IP — technically still "public-facing"
  ❌ Traffic from on-prem over VPN can't reach the service via this path
  ❌ Not as private as Private Endpoint
```

### Service Endpoints available for
```
Microsoft.Storage      → Azure Storage, ADLS Gen2
Microsoft.Sql          → Azure SQL Database, Synapse
Microsoft.KeyVault     → Azure Key Vault
Microsoft.CosmosDB     → Cosmos DB
Microsoft.ServiceBus   → Service Bus
Microsoft.EventHub     → Event Hub
Microsoft.Web          → Azure App Service
```

---

## 12. Private Endpoint & Private Link

### What is it?
A Private Endpoint gives an Azure PaaS service **a private IP inside YOUR VNet**. Traffic to that service flows entirely within your VNet — no public IP involved.

### The difference from Service Endpoint
```
SERVICE ENDPOINT:
  Your VNet ──────────────────────────────► ADLS public IP (20.150.x.x)
                  Azure backbone             (service still "public")
  VNet identity passed in header

PRIVATE ENDPOINT:
  Your VNet ──► NIC (10.0.4.9) ──► ADLS    (via Azure backbone, no public IP)
                private IP in         (service APPEARS to be at 10.0.4.9)
                your subnet
  ADLS public access can be COMPLETELY disabled
```

### How Private Endpoint works — anatomy
```
When you create a Private Endpoint for ADLS account "adlsprod":

1. Azure creates a Network Interface (NIC) inside your chosen subnet.
   NIC gets a private IP: 10.0.4.9

2. This NIC is "connected" to adlsprod via Azure's backbone.
   When traffic reaches 10.0.4.9, it's forwarded to adlsprod internally.

3. A DNS record must map adlsprod.dfs.core.windows.net → 10.0.4.9
   (otherwise DNS returns the public IP and traffic bypasses the PE)

4. You can then disable public access on adlsprod:
   Storage Account → Networking → "Disable public access"
   Now ONLY the private endpoint path works.

┌─────────────────────────────────────────────────────────────────┐
│ YOUR VNet: 10.0.0.0/16                                          │
│                                                                  │
│  VM (10.0.1.4)                                                   │
│       │                                                          │
│       │  DNS: adlsprod.dfs.core.windows.net → 10.0.4.9          │
│       │  (resolved via Private DNS Zone)                         │
│       ▼                                                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ pe-subnet: 10.0.4.0/24                                    │   │
│  │                                                            │   │
│  │  NIC: pe-adlsprod-nic  IP: 10.0.4.9                       │   │
│  │       ↑ this is the Private Endpoint                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                    │                                              │
│                    │ Azure internal backbone (no internet)        │
│                    ▼                                              │
│  ADLS Gen2: adlsprod  (public access disabled)                   │
└─────────────────────────────────────────────────────────────────┘
```

### Private DNS Zone — critical piece
```
Without Private DNS Zone, the DNS lookup bypasses your private endpoint:

  dig adlsprod.dfs.core.windows.net
  → returns: 20.150.x.x  (PUBLIC IP) ← traffic goes around your PE!

With Private DNS Zone linked to your VNet:
  Private DNS Zone: privatelink.dfs.core.windows.net
  Record:  adlsprod  →  10.0.4.9  (your PE private IP)

  dig adlsprod.dfs.core.windows.net
  → Azure DNS sees CNAME: adlsprod.privatelink.dfs.core.windows.net
  → Resolves in Private DNS Zone
  → returns: 10.0.4.9  ✅ (traffic flows through PE)
```

### Private DNS Zones for common services
```
Service                           Private DNS Zone
────────────────────────────────  ────────────────────────────────────────
ADLS Gen2 (DFS endpoint)          privatelink.dfs.core.windows.net
Blob Storage                      privatelink.blob.core.windows.net
Azure SQL Database                privatelink.database.windows.net
Azure Key Vault                   privatelink.vaultcore.azure.net
Azure Service Bus                 privatelink.servicebus.windows.net
Azure Event Hub                   privatelink.servicebus.windows.net
Azure Container Registry          privatelink.azurecr.io
Azure Databricks workspace        privatelink.azuredatabricks.net
Azure Cosmos DB (SQL)             privatelink.documents.azure.com
Azure Monitor                     privatelink.monitor.azure.com
```

### Private Link Service (expose your OWN service privately)
```
Private Link Service lets YOU expose YOUR OWN app (running behind a Load Balancer)
as a Private Link so OTHER VNets/subscriptions can connect to it privately.

Use case: You run a SaaS platform. Your customer wants to connect to your service
          without exposing it to the internet.

Your infrastructure:
  VNet-Provider: 10.0.0.0/16
  └── Internal Load Balancer → your app VMs
  └── Private Link Service: linked to that Load Balancer

Customer infrastructure:
  VNet-Consumer: 10.2.0.0/16
  └── Private Endpoint → points to your Private Link Service
      Gets private IP: 10.2.5.9

Result:
  Customer app (10.2.1.4) → 10.2.5.9 → Private Link Service → your app ✅
  No peering needed, no VNet overlap issues.
```

---

## 13. Azure Firewall

### What is it?
Azure Firewall is a **managed, stateful firewall service** that inspects, logs, and filters ALL traffic passing through a hub VNet. More powerful than NSGs.

```
NSG vs Azure Firewall:
┌─────────────────┬────────────────────────────┬───────────────────────────┐
│ Feature         │ NSG                         │ Azure Firewall            │
├─────────────────┼────────────────────────────┼───────────────────────────┤
│ Layer           │ Layer 4 (TCP/UDP/ICMP)      │ Layer 4 + Layer 7 (FQDN) │
│ FQDN filtering  │ ❌ No                       │ ✅ Yes (*.pypi.org)       │
│ Logging         │ Flow logs only              │ Full packet logs, Azure Monitor│
│ Threat intel    │ ❌ No                       │ ✅ Microsoft threat feeds │
│ NAT rules       │ ❌ No                       │ ✅ DNAT (inbound NAT)     │
│ Managed service │ ✅ Yes                      │ ✅ Yes                    │
│ Cost            │ Free (NSG is free)          │ $1.25/hour + data fees   │
│ Scope           │ Subnet or NIC               │ Hub VNet (centralized)    │
│ TLS inspection  │ ❌ No                       │ ✅ Yes (Premium tier)     │
└─────────────────┴────────────────────────────┴───────────────────────────┘
```

### Azure Firewall rule types
```
1. APPLICATION RULES (Layer 7, FQDN-based)
   Allow outbound to specific websites/APIs by domain name.
   Example: Allow VMs to reach *.pypi.org on port 443
            Allow VMs to reach *.dfs.core.windows.net on port 443
            Deny VMs from reaching *.torrent-site.com

2. NETWORK RULES (Layer 4, IP/port-based)
   Like NSG rules but centralized.
   Example: Allow 10.0.1.0/24 → 10.1.2.0/24 on port 5432 (PostgreSQL)
            Allow 10.0.0.0/8 → 8.8.8.8 on port 53 (DNS)

3. DNAT RULES (inbound traffic translation)
   Translate inbound public IP:port to internal private IP:port.
   Example: Public IP 52.x.x.x:8080 → VM 10.0.1.4:80
```

### Hub-Spoke with Azure Firewall
```
Enterprise pattern: all VNets route through a central hub firewall.

┌─────────────────────────────────────────────────────────────────────┐
│ HUB VNet (vnet-hub: 10.0.0.0/16)                                    │
│   AzureFirewallSubnet: 10.0.0.0/26                                   │
│   Azure Firewall: 10.0.0.4  (private IP)                             │
│   Public IP: 20.x.x.x                                                │
└───────────┬──────────────────┬────────────────────┬──────────────────┘
            │ Peering          │ Peering             │ Peering
     ┌──────▼────┐     ┌───────▼───┐     ┌──────────▼──┐
     │ Spoke-1    │     │ Spoke-2   │     │ Spoke-3      │
     │ Dev VNet   │     │ Prod VNet │     │ Analytics    │
     │10.1.0.0/16 │     │10.2.0.0/16│     │10.3.0.0/16   │
     └────────────┘     └───────────┘     └─────────────┘

  Route tables on all spoke subnets:
    0.0.0.0/0 → Azure Firewall (10.0.0.4)
  
  This forces ALL traffic (internet, spoke-to-spoke) through the firewall.
  Firewall logs and filters everything.
  Spoke-1 → Spoke-2 = Spoke-1 → Firewall → Spoke-2 (inspected) ✅
```

---

## 14. User Defined Routes (UDR) & Route Tables

### What is it?
Azure uses **system routes** by default (traffic between subnets in same VNet, to internet, etc.). You can **override** these with UDRs to control where traffic goes.

```
Default system routes (Azure adds these automatically):
  Destination         Next Hop
  ─────────────────   ──────────────────
  10.0.0.0/16         VirtualNetwork    (traffic stays in VNet)
  0.0.0.0/0           Internet          (all other traffic → internet)
  10.1.0.0/16         VirtualNetwork    (peered VNet added automatically)
```

```
Custom UDR example — force all internet traffic through firewall:
  Route Table: rt-spoke1
  ┌───────────────────┬─────────────────────────────────────────┐
  │ Destination       │ Next Hop                                  │
  ├───────────────────┼─────────────────────────────────────────┤
  │ 0.0.0.0/0         │ Virtual Appliance: 10.0.0.4 (Firewall)   │
  │ 10.2.0.0/16       │ Virtual Appliance: 10.0.0.4 (Firewall)   │
  └───────────────────┴─────────────────────────────────────────┘
  Attach to: Spoke-1 subnets

  Now when a VM in Spoke-1 tries to reach internet (0.0.0.0/0):
  → Azure checks route table: next hop is 10.0.0.4 (Firewall)
  → Traffic goes to Firewall, which applies its rules, then forwards to internet
  → Traffic is inspected/logged
```

### Common UDR use cases
```
1. Force internet traffic through Azure Firewall (hub-spoke model)
2. Force traffic to on-prem through a VPN appliance (not the VPN Gateway)
3. Prevent traffic from going to certain subnets (Next Hop: None = drop)
4. Split tunneling in P2S VPN (send only Azure traffic through VPN)
```

---

## 15. Application Gateway & Load Balancer

### Azure Load Balancer (Layer 4)
```
Distributes TCP/UDP traffic across multiple backend VMs.
Works at transport layer — doesn't understand HTTP content.

Use case: 5 web server VMs behind one public IP.
  Client → Load Balancer (public IP 52.x.x.x:80)
  → Round-robin to VM1:80, VM2:80, VM3:80, VM4:80, VM5:80

Types:
  BASIC → dev/test, no SLA, 1 backend pool
  STANDARD → production, zone-redundant, SLA 99.99%, NAT rules
```

### Azure Application Gateway (Layer 7)
```
An HTTP/HTTPS load balancer that understands web traffic.
Can route based on URL path, host header.
Has built-in WAF (Web Application Firewall).

Use case: Single entry point for multiple backend services:
  Client → App Gateway (public IP)
  /api/*      → API servers (10.0.2.4, 10.0.2.5)
  /static/*   → CDN or storage
  /admin/*    → Admin servers (10.0.3.4) + WAF rules

WAF (Web Application Firewall):
  Blocks: SQL injection, XSS, CSRF, OWASP Top 10 attacks
  Modes: Detection (log only) vs Prevention (block + log)
```

---

## 16. DNS in Azure

### How Azure DNS works by default
```
When you create a VNet, Azure provides DNS automatically:
  DNS server: 168.63.129.16  (Azure's "magic IP" — always accessible from any VNet)

  This resolves:
  ├── Azure public DNS (google.com, pypi.org, etc.)
  ├── Azure service public hostnames (storageaccount.blob.core.windows.net → public IP)
  └── Private DNS Zones linked to your VNet (if you've created them)
```

### Private DNS Zone
```
A Private DNS Zone is a DNS zone that ONLY answers queries from VNets you link it to.
External DNS servers can't query it.

Example:
  Private DNS Zone: privatelink.dfs.core.windows.net
  Record: adlsprod → 10.0.4.9  (PE's private IP)
  Linked to: vnet-prod

  VM in vnet-prod queries: adlsprod.dfs.core.windows.net
    → Azure DNS (168.63.129.16) checks Private DNS Zones linked to this VNet
    → Finds: adlsprod → 10.0.4.9
    → Returns 10.0.4.9 ✅  (private IP, traffic goes through PE)

  VM NOT in vnet-prod (or someone from internet) queries: adlsprod.dfs.core.windows.net
    → Public DNS returns: 20.150.x.x (real public IP)
    → But storage account firewall denies it anyway ✅
```

### Custom DNS Server (for on-prem hybrid scenarios)
```
When you have on-prem DNS and Azure DNS:

PROBLEM:
  On-prem VM queries your corporate DNS (192.168.0.1) for adlsprod.dfs.core.windows.net
  Corporate DNS doesn't know about Azure Private DNS Zones
  → Returns public IP → traffic goes over internet (not through PE)

SOLUTION: DNS Forwarder in Azure

  ┌─────────────────┐        ┌───────────────────────┐
  │ On-Prem DNS     │        │ Azure DNS Forwarder VM │
  │ 192.168.0.1     │        │ 10.0.6.4               │
  │                 │        │ (forwarder to          │
  │ For *.azure,    │──────► │  168.63.129.16)        │
  │ forward to      │        │                        │
  │ 10.0.6.4        │        └───────────────────────┘
  └─────────────────┘                   │
                                        ▼
                                Azure DNS (168.63.129.16)
                                  → Private DNS Zone
                                  → returns 10.0.4.9 ✅

On-prem VM DNS lookup:
  → corporate DNS (192.168.0.1)
  → forward *.dfs.core.windows.net to Azure forwarder (10.0.6.4)
  → Azure forwarder asks 168.63.129.16
  → Private DNS Zone returns 10.0.4.9
  → On-prem VM reaches ADLS via Private Endpoint over VPN! ✅
```

---

## 17. How Resource Groups Fit into Networking

### Resource Groups are NOT network boundaries
```
Common misunderstanding:
  "If my VMs are in different Resource Groups, they can't talk to each other."
  This is WRONG.

Resource Group = Administrative/billing container
VNet/Subnet    = Network boundary

A Resource Group:
  ✅ Groups resources for RBAC (access control)
  ✅ Groups resources for billing and cost management
  ✅ Groups resources for lifecycle (delete RG = delete all resources in it)
  ❌ Has NO effect on network routing
  ❌ Has NO firewall or isolation behavior
```

### Cross-resource-group network scenarios
```
Scenario A: VM in RG-App, VNet in RG-Network
  ─────────────────────────────────────────
  This is valid and common (hub-spoke pattern):
    RG-Network:  vnet-prod (owns the VNet and subnets)
    RG-App:      vm-webserver (deployed into vnet-prod's web-tier subnet)
    RG-DB:       azure-sql (uses vnet-prod's data-tier subnet via Private Endpoint)

  vm-webserver → azure-sql (via PE)  ✅ — network doesn't care about RG

Scenario B: Two VMs in different RGs, same VNet
  ─────────────────────────────────────────────
  RG-Frontend:  vm-frontend (10.0.1.4 in web-tier)
  RG-Backend:   vm-backend  (10.0.2.5 in app-tier)
  Both in vnet-prod.

  vm-frontend → vm-backend  ✅ — same VNet, NSG allows it

Scenario C: VM in RG-A communicates to Storage in RG-B
  ─────────────────────────────────────────────────
  RG-Compute:   vm-processor (10.0.1.4)
  RG-Storage:   adlsprod (in same subscription, different RG)

  Created Private Endpoint for adlsprod in pe-subnet (10.0.4.9)
  vm-processor → 10.0.4.9 (PE) → adlsprod  ✅
  RG boundary is completely irrelevant to this traffic.
```

---

## 18. Communication Scenarios — All Patterns

### Pattern 1: Two VMs in the same VNet
```
Setup:
  VNet: 10.0.0.0/16
  Subnet A: 10.0.1.0/24  — VM-A: 10.0.1.4
  Subnet B: 10.0.2.0/24  — VM-B: 10.0.2.5

Communication:
  VM-A → VM-B (10.0.2.5:5432)
  Route: Azure system route (VirtualNetwork → VirtualNetwork)
  Firewall check: NSG on Subnet B (must allow inbound from 10.0.1.0/24:5432)
  ✅ Works by default (if NSG allows it)
```

### Pattern 2: Two VMs in different VNets
```
Setup:
  VNet-A: 10.0.0.0/16  — VM-A: 10.0.1.4
  VNet-B: 10.1.0.0/16  — VM-B: 10.1.1.5

Without peering: VM-A → VM-B ❌ (no route)

With peering (VNet-A ↔ VNet-B):
  VM-A (10.0.1.4) → VM-B (10.1.1.5)
  Route: peering route (added to both VNets' route tables automatically)
  NSG check on VM-B's subnet
  ✅ Works

Traffic path:
  VM-A NIC → VNet-A routing → VNet Peering link → VNet-B routing → VM-B NIC
  No internet. Azure backbone only.
```

### Pattern 3: VM to Azure PaaS (Storage, SQL, Key Vault)
```
Option A — Public endpoint (least secure):
  VM → DNS → public IP → ADLS  (traffic may go over internet)

Option B — Service Endpoint (medium):
  VM → DNS → public IP of ADLS (BUT via Azure backbone, not internet)
  ADLS firewall allows VM's subnet

Option C — Private Endpoint (most secure):
  VM → DNS (Private DNS Zone) → 10.0.4.9 (PE) → ADLS
  All traffic stays in VNet. ADLS public access can be disabled.
```

### Pattern 4: VM to VM in different subscriptions
```
Setup:
  Sub-A: VNet-A: 10.0.0.0/16  — VM-A
  Sub-B: VNet-B: 10.1.0.0/16  — VM-B

Cross-subscription VNet Peering:
  You can peer across subscriptions as long as:
  ├── Both VNets' address spaces don't overlap
  ├── The user setting up peering has "Network Contributor" role on BOTH VNets
  └── Both are in the SAME Azure AD tenant (or different tenants with special setup)

Steps:
  1. In Sub-A: peer VNet-A → VNet-B (status: Initiated)
  2. In Sub-B: peer VNet-B → VNet-A (status: Connected)
  → VM-A can now reach VM-B on private IP ✅
```

### Pattern 5: App in Azure to on-prem database
```
Setup:
  Azure VNet: 10.0.0.0/16
  On-Prem:    192.168.0.0/24
  VPN Gateway Site-to-Site tunnel established

  Azure VM (10.0.1.4) needs to reach on-prem Oracle DB (192.168.0.50:1521)

Traffic path:
  VM (10.0.1.4) → route table (192.168.0.0/24 via VPN Gateway)
  → VPN Gateway → encrypted IPsec tunnel → on-prem VPN device → Oracle DB (192.168.0.50)

NSG check: Azure NSG must allow outbound from subnet to 192.168.0.0/24:1521
On-prem firewall check: must allow inbound from Azure IP range to Oracle port
✅ Works
```

---

## 19. Storage Access Scenarios

### Scenario A — VM reads ADLS (same VNet, Private Endpoint)
```
Actor: vm-etl (10.0.1.4) in vnet-prod
Target: adlsprod.dfs.core.windows.net (ADLS Gen2)

Step 1: DNS Lookup
  vm-etl asks Azure DNS: "What is adlsprod.dfs.core.windows.net?"
  Azure DNS checks Private DNS Zone (privatelink.dfs.core.windows.net)
  Returns: 10.0.4.9  (private endpoint IP)

Step 2: TCP connection
  vm-etl → 10.0.4.9:443 (HTTPS)
  Route: within VNet → pe-subnet
  NSG check: NSG on pe-subnet (allow inbound 443 from 10.0.1.0/24)

Step 3: Authentication
  Request carries Bearer token (OAuth, from Managed Identity or SP)
  ADLS validates token: "Does this identity have Storage Blob Data Reader?" ✅

Step 4: Data transfer
  ADLS → vm-etl: parquet file data flows back over same path
  vm-etl processes data

Result: Traffic NEVER left your VNet. ADLS public access can be fully disabled.
```

### Scenario B — On-prem server reads ADLS (via VPN + Private Endpoint)
```
This is the tricky one — Private Endpoint with on-prem access.

Actor: on-prem server (192.168.0.20)
Target: adlsprod.dfs.core.windows.net

Problem: On-prem server's DNS (192.168.0.1) resolves to PUBLIC IP of ADLS!
         → traffic goes over internet, ADLS firewall blocks it.

Fix — Set up DNS forwarding:

Step 1: Create Azure DNS Private Resolver (or a VM-based forwarder)
  In Azure subnet: DNS forwarder at 10.0.6.4
  Config: forward *.blob.core.windows.net, *.dfs.core.windows.net → 168.63.129.16

Step 2: Configure on-prem DNS conditional forwarder
  On-prem DNS server: if query ends with *.dfs.core.windows.net
  → forward to 10.0.6.4 (Azure DNS forwarder, reachable via VPN)

Step 3: Traffic flow
  on-prem server queries: adlsprod.dfs.core.windows.net
  → on-prem DNS → "forward to 10.0.6.4"
  → Azure DNS Resolver → 168.63.129.16 → Private DNS Zone
  → returns: 10.0.4.9 (PE private IP)

  on-prem server connects to: 10.0.4.9:443
  Route: 10.0.4.x → via VPN tunnel → VPN Gateway → vnet-prod pe-subnet (10.0.4.9)
  → Private Endpoint → ADLS

Result: On-prem server reads ADLS privately via VPN.
        Traffic: on-prem → VPN tunnel (encrypted) → Azure PE → ADLS ✅
```

### Scenario C — Databricks cluster reads ADLS (VNet injected)
```
Already covered in detail in badatabricks_netwroking.md — summary:

Cluster VM (10.0.3.5) → DNS resolves to PE IP → ADLS PE (10.0.4.9)
→ Bearer token from Azure AD for Managed Identity
→ ADLS validates token (Storage Blob Data Contributor role)
→ Data flows back to cluster VM
→ Spark distributes to executor workers
```

### Scenario D — Azure Function reads Blob Storage (no VNet)
```
Simple scenario — Azure Function App without VNet integration:

Function App (serverless, no fixed IP) → Blob Storage public endpoint
→ Authentication via Managed Identity or connection string
→ Storage firewall: "Allow Azure services" checked ✅

Less secure (other Azure services could also access it).
Better: Put Function App in a VNet with VNet Integration + Private Endpoint.
```

### Scenario E — Storage Account Firewall Options
```
Azure Storage Account has its own firewall layer:

┌──────────────────────────────────────────────────────────────────────┐
│ Storage Account: adlsprod → Networking settings                       │
│                                                                        │
│ Public network access:                                                 │
│   ○ Enabled from all networks     ← wide open, not recommended        │
│   ○ Enabled from selected VNets   ← Service Endpoints or PE allow     │
│   ● Disabled                      ← Private Endpoints ONLY ✅          │
│                                                                        │
│ If "selected VNets" chosen:                                            │
│   Virtual networks (Service Endpoint):                                  │
│     + vnet-prod / web-tier subnet                                      │
│     + vnet-prod / data-tier subnet                                     │
│   IP rules (individual IPs or CIDR):                                   │
│     + 52.100.1.1  (NAT Gateway IP of your Databricks subnets)         │
│   Allow trusted Microsoft services:  ✅ Yes                            │
│     (Azure Backup, Data Factory, etc. can bypass firewall)             │
│                                                                        │
│ Private endpoint connections:                                           │
│   pe-adls-prod: Approved ✅                                            │
│   pe-adls-serverless: Approved ✅                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 20. On-Premises Connectivity Scenarios

### Scenario 1 — On-prem to Azure VM via VPN (Site-to-Site)
```
Goal: Developer in office accesses an internal Azure VM for testing.
      The VM has NO public IP.

Network topology:
  Office: 192.168.1.0/24
  Azure VNet: 10.0.0.0/16
  Azure VM: 10.0.1.10 (no public IP, only private)

Components set up:
  1. VPN device on-prem (Cisco router, public IP: 203.0.113.1)
  2. Azure VPN Gateway (GatewaySubnet: 10.0.5.0/27, Public IP: 20.100.1.1)
  3. Local Network Gateway in Azure:
     → defines: on-prem range = 192.168.1.0/24, gateway IP = 203.0.113.1
  4. Connection: Site-to-Site, shared key configured on both sides

After tunnel is up:
  Office PC (192.168.1.50) → SSH to 10.0.1.10
  Traffic path:
    192.168.1.50 → office router → recognizes 10.0.x.x is Azure subnet
    → routes to Cisco VPN → IPsec encrypted tunnel
    → Azure VPN Gateway → routes to 10.0.1.0/24 subnet
    → VM 10.0.1.10 receives SSH connection ✅

NSG on VM's subnet must allow: inbound SSH (port 22) from 192.168.1.0/24
```

### Scenario 2 — On-prem to Azure Storage via Private Endpoint over VPN
```
Covered in Scenario B above (Section 19).
Key requirement: DNS forwarding via Azure DNS resolver.
```

### Scenario 3 — Azure VM to on-prem SQL Server
```
Goal: Azure data processing VM writes results to on-prem SQL Server.

Network:
  Azure VM: 10.0.1.4 (in vnet-prod)
  On-prem SQL: 192.168.0.100:1433
  S2S VPN tunnel: Azure ↔ on-prem (already up)

Route on Azure side:
  Azure VM's route table includes:
  192.168.0.0/24 → via VPN Gateway  (added automatically when S2S connection is active)

On-prem firewall:
  Allow: inbound port 1433 from 10.0.0.0/16 (Azure VNet range)

Azure NSG:
  Allow: outbound port 1433 from web-tier subnet to 192.168.0.0/24

Traffic:
  VM (10.0.1.4) → route to VPN GW → encrypted tunnel → on-prem → SQL Server ✅
```

### Scenario 4 — ExpressRoute to Azure Storage (Private Peering)
```
Large enterprise using ExpressRoute:

  On-prem DC (192.168.0.0/16)
  ↓ Fiber to ISP
  ExpressRoute Circuit (10 Gbps, private)
  ↓
  Azure ExpressRoute Gateway (in GatewaySubnet)
  ↓
  Azure VNet: 10.0.0.0/16
  ↓
  ADLS Private Endpoint: 10.0.4.9

Traffic: on-prem VM → private circuit → Azure → Private Endpoint → ADLS
  ✅ No internet at any point
  ✅ Consistent 10 Gbps, sub-10ms latency
  ✅ SLA 99.95% uptime

When to use ExpressRoute over VPN:
  - Need to move 10s of TB regularly (ETL, backup)
  - Compliance requires no internet path (HIPAA, PCI-DSS, SOX)
  - Need guaranteed bandwidth SLA
```

### Scenario 5 — Employee VPN (Point-to-Site) to access internal resources
```
Goal: Remote employee accesses internal Azure and on-prem resources from laptop.

Azure P2S VPN setup:
  VPN Gateway with P2S config:
    Auth: Azure AD (employee logs in with corporate credentials)
    Client pool: 172.16.0.0/24
    DNS: custom DNS server in Azure

Employee's laptop gets VPN client (Azure VPN Client app):
  Connects → authenticates with Azure AD
  Laptop gets: 172.16.0.55 (from P2S pool)

Now the employee can reach:
  10.0.1.x (Azure VMs)               ✅ via VPN tunnel → VNet routing
  10.0.4.9 (ADLS Private Endpoint)   ✅ via VPN → PE
  192.168.0.x (on-prem over VPN GW)  ✅ via VPN → Azure VPN GW → S2S tunnel → on-prem

Split tunneling (optional):
  Only Azure/on-prem traffic goes through VPN.
  Internet traffic (YouTube, Gmail) bypasses VPN → better performance.
```

---

## 21. Full Architecture Example — Enterprise Setup

```
COMPANY: Contoso Analytics
REQUIREMENT: Secure data platform. Databricks, ADLS, SQL. On-prem connectivity.
             All traffic private. No direct public internet from VMs.

┌─────────────────────────────────────────────────────────────────────────────┐
│ Azure Subscription: sub-contoso-prod                                         │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ HUB VNet: vnet-hub (10.0.0.0/16) — East US                           │   │
│  │                                                                        │   │
│  │  AzureFirewallSubnet (10.0.0.0/26)                                    │   │
│  │    └── Azure Firewall: 10.0.0.4                                        │   │
│  │        Public IP: 20.100.1.1                                           │   │
│  │        FQDN Rules: Allow *.dfs.core.windows.net, *.pypi.org           │   │
│  │        Block: all others                                                │   │
│  │                                                                        │   │
│  │  GatewaySubnet (10.0.1.0/27)                                          │   │
│  │    ├── VPN Gateway (S2S to on-prem office 192.168.0.0/24)             │   │
│  │    └── ExpressRoute Gateway (to on-prem DC)                           │   │
│  │                                                                        │   │
│  │  dns-subnet (10.0.2.0/28)                                             │   │
│  │    └── Azure DNS Private Resolver: 10.0.2.4                           │   │
│  │        Inbound: receives on-prem DNS queries                          │   │
│  │        Outbound: forwards *.privatelink.* to 168.63.129.16            │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                         │                    │                                │
│               Peering (hub↔spoke)   Peering (hub↔spoke)                      │
│                         │                    │                                │
│  ┌──────────────────────▼──────┐  ┌─────────▼───────────────────────────┐  │
│  │ SPOKE-1: vnet-databricks     │  │ SPOKE-2: vnet-data                   │  │
│  │ (10.1.0.0/16)                │  │ (10.2.0.0/16)                        │  │
│  │                              │  │                                       │  │
│  │ dbr-public-subnet 10.1.1.0/26│  │ pe-subnet: 10.2.1.0/24               │  │
│  │   Databricks driver VMs      │  │   pe-adls:    10.2.1.9               │  │
│  │                              │  │   pe-keyvault:10.2.1.10              │  │
│  │ dbr-private-subnet10.1.2.0/26│  │   pe-sql:     10.2.1.11             │  │
│  │   Databricks worker VMs      │  │                                       │  │
│  │                              │  │ ADLS Gen2: adlsprod                   │  │
│  │ Route Table: all→ FW 10.0.0.4│  │   Firewall: deny public ✅            │  │
│  │ NAT Gateway: 52.x.x.x        │  │   Auth: Managed Identity             │  │
│  └──────────────────────────────┘  │                                       │  │
│                                    │ Azure SQL: sqlprod                    │  │
│                                    │   Firewall: deny public ✅            │  │
│                                    │                                       │  │
│                                    └───────────────────────────────────────┘ │
│                                                                               │
│  Private DNS Zones (linked to HUB + all spokes):                             │
│    privatelink.dfs.core.windows.net   → 10.2.1.9                            │
│    privatelink.vaultcore.azure.net    → 10.2.1.10                           │
│    privatelink.database.windows.net  → 10.2.1.11                           │
│                                                                               │
│  Unity Catalog:                                                               │
│    Access Connector: adb-connector-prod                                       │
│    Managed Identity: mi-uc-prod                                               │
│    Role: Storage Blob Data Contributor on adlsprod                           │
└─────────────────────────────────────────────────────────────────────────────┘
                    │                    │
              VPN/ExpressRoute     VPN P2S
                    │                    │
┌───────────────────▼────────┐  ┌───────▼──────────────────┐
│ On-Prem Data Center         │  │ Remote Employees          │
│ 192.168.0.0/16              │  │ Laptops + Azure VPN Client│
│                             │  │ Pool: 172.16.0.0/24       │
│ DNS: 192.168.0.1            │  └──────────────────────────┘
│   Forwarder: *.privatelink.*│
│   → Azure DNS Resolver      │
│     10.0.2.4 (hub)         │
│                             │
│ App Server (192.168.0.50)   │
│   → reads adlsprod via PE   │
│   → DNS resolves to 10.2.1.9│
│   → traffic via ExpressRoute│
└─────────────────────────────┘

Traffic flows in this setup:
  Databricks cluster → ADLS:
    10.1.2.4 → DNS → 10.2.1.9 → ADLS  (via VNet peering hub→spoke2)

  On-prem server → ADLS:
    192.168.0.50 → DNS forwarder → 10.0.2.4 → 10.2.1.9 → ADLS
    (over ExpressRoute, fully private)

  Developer laptop → Databricks UI:
    P2S VPN → Private Endpoint for Databricks workspace (if configured)

  All outbound internet from clusters:
    10.1.x.x → UDR → Azure Firewall (10.0.0.4) → filtered → internet
```

---

## 22. Decision Trees — Which Networking Tool to Use

### "How should resources communicate?"
```
Is the resource in the SAME VNet?
  YES → Use private IPs directly (ensure NSG allows it)
  NO  →
    Is it in a different VNet in the SAME region?
      YES → VNet Peering (simplest, lowest latency)
      NO  →
        Different region?
          YES → Global VNet Peering (higher latency, higher cost per GB)
        Different subscription or tenant?
          YES → Cross-subscription peering (requires proper RBAC on both VNets)
        Need full isolation + encrypted?
          YES → VNet-to-VNet VPN Gateway (encrypted, more overhead)
```

### "How should I connect to an Azure PaaS service?"
```
How sensitive is your data? / What are your compliance requirements?

Low → Service Endpoint (simple, stays on Azure backbone, medium security)

Medium → Private Endpoint (fully private, PaaS gets private IP in your VNet)

Regulated (banking/healthcare) → Private Endpoint + disable all public access
                                  + Private DNS Zones + DNS forwarder for on-prem
```

### "How should I connect on-prem to Azure?"
```
Dev/small office (< 10 people, non-critical):
  → Point-to-Site VPN (each developer installs VPN client)

Office/branch to Azure (site-level, <1 Gbps):
  → Site-to-Site VPN Gateway
  → Requires on-prem VPN device

High bandwidth, compliance, mission-critical:
  → ExpressRoute (10/100 Gbps, private circuit, SLA)
```

### "How should outbound internet traffic work for VMs?"
```
Dev VMs, low security:    Public IP on VM (easy but exposed)
Prod VMs, standard:       NAT Gateway (stable outbound IP, no inbound)
Enterprise, inspect all:  Azure Firewall + UDR (log/filter everything)
```

---

## 23. Quick Reference Cheat Sheet

### Networking objects — one-liner descriptions
```
Object                  What it does in one line
──────────────────────  ────────────────────────────────────────────────────────
VNet                    Private isolated network in Azure region
Subnet                  Segment of VNet address space; resources deploy here
NSG                     Stateful L4 firewall; allow/deny rules per subnet or NIC
NIC                     VM's virtual network adapter; holds private (+ optional public) IP
Public IP               Internet-routable IP attached to NIC or Load Balancer
NAT Gateway             Shared stable outbound IP for subnets (outbound only)
VNet Peering            Connects two VNets so they can use private IPs (non-transitive)
VPN Gateway (S2S)       Encrypted IPsec tunnel between VNet and on-prem network
VPN Gateway (P2S)       Encrypted tunnel for individual devices (remote workers)
ExpressRoute            Private dedicated circuit to Azure, no internet
Service Endpoint        Route VNet traffic to PaaS via Azure backbone (service still public)
Private Endpoint        Give PaaS a private IP in YOUR subnet (fully private, disable public)
Private Link            Backend connection mechanism powering Private Endpoints
Private DNS Zone        DNS zone only visible inside linked VNets; maps PE IPs
Azure Firewall          Managed L4/L7 firewall; FQDN filtering, logging, hub-spoke
Route Table / UDR       Override Azure default routing; force traffic via firewall/appliance
Load Balancer (L4)      Distribute TCP/UDP traffic across backend VMs
App Gateway (L7)        HTTP load balancer; URL routing, WAF, SSL termination
Azure Bastion           Browser-based SSH/RDP to VMs without Public IP
DNS Private Resolver    Managed inbound/outbound DNS forwarding (hybrid DNS)
```

### Port reference
```
Port   Protocol   Service
─────  ─────────  ─────────────────────────────
22     TCP        SSH (Linux VM admin)
3389   TCP        RDP (Windows VM admin)
443    TCP        HTTPS (web, APIs, Azure services)
80     TCP        HTTP (web)
1433   TCP        SQL Server / Azure SQL
5432   TCP        PostgreSQL
3306   TCP        MySQL
6379   TCP        Redis
8080   TCP        HTTP alternative / many apps
9000   TCP        Databricks init (clusters)
```

### CIDR cheat sheet
```
CIDR   IPs total   Usable IPs (Azure reserves 5)
/16    65,536      65,531  ← VNet level
/24    256         251     ← Standard subnet
/26    64          59      ← Small subnet (Databricks minimum)
/27    32          27      ← Gateway subnet
/28    16          11      ← Tiny (PE subnet)
/29    8           3       ← Too small for most things
```

### Traffic flow — who checks what
```
VM-A (sends) → NSG(NIC-A outbound) → NSG(Subnet-A outbound) → Azure routing
             → NSG(Subnet-B inbound) → NSG(NIC-B inbound)  → VM-B (receives)
```

### Common mistakes
```
❌ Overlapping IP address spaces — VNets you want to peer CANNOT have overlapping CIDRs
❌ Missing Private DNS Zone — Private Endpoint works but DNS returns public IP
❌ NSG blocks traffic between subnets — Forgot to allow the new app tier in the DB NSG
❌ Service Endpoint without storage firewall rule — endpoint active but storage still open
❌ VNet Peering non-transitive — A↔B, B↔C doesn't mean A→C
❌ GatewaySubnet named wrong — Must be exactly "GatewaySubnet" (case-sensitive)
❌ Dynamic private IP on a server — IP changes on restart, breaking DNS and connection strings
❌ On-prem DNS not forwarding privatelink zones — PE exists but on-prem resolves public IP
❌ Firewall blocks HTTPS outbound — Clusters can't download pip packages, init scripts fail
❌ NSG on GatewaySubnet with wrong rules — Can break VPN/ER connections
```

---

## 24. Cost Reference — Free vs Chargeable

> All prices are approximate East US rates (June 2026). Always verify on the Azure Pricing Calculator.
> URL: https://azure.microsoft.com/en-us/pricing/calculator/

### FREE networking resources
```
Resource                     Free? Notes
───────────────────────────  ───── ──────────────────────────────────────────────
VNet                         ✅    Creating a VNet is completely free
Subnet                       ✅    Creating subnets is free
NSG                          ✅    NSG itself is free; only data transfer costs
Route Table / UDR            ✅    Creating route tables and rules is free
Service Endpoint             ✅    Enabling service endpoints is free
                                   (you still pay for data transfer to the service)
Private DNS Zone — queries   ✅    First 1 billion queries/month free
VNet Peering — same region   ❌*  Inbound + outbound data transfer charged (see below)
                                   * The peering link itself is free; traffic is charged
VNet-to-VNet traffic         ❌    See peering costs below
Azure DNS (default)          ✅    168.63.129.16 resolver is free for VNet resources
```

### Chargeable networking resources — with approximate costs

```
┌──────────────────────────────┬─────────────────────────────────────────────────────────┐
│ Resource                     │ Approximate Cost (East US)                               │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ PUBLIC IP ADDRESS            │                                                           │
│   Standard Static            │ ~$0.005/hour = ~$3.65/month per IP                      │
│   Standard Dynamic           │ ~$0.004/hour = ~$2.92/month per IP                      │
│   Basic (retiring)           │ ~$0.004/hour                                             │
│                              │ TIP: Delete unused public IPs — they still cost money   │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ NAT GATEWAY                  │                                                           │
│   Gateway resource           │ ~$0.045/hour = ~$32/month                               │
│   Data processed             │ ~$0.045/GB outbound through NAT                         │
│   Example: 1 NAT GW + 100GB  │ ~$32 + $4.50 = ~$36.50/month                           │
│                              │ TIP: One NAT Gateway can serve multiple subnets         │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ VNET PEERING                 │                                                           │
│   Same region (both dirs)    │ ~$0.01/GB inbound + $0.01/GB outbound                   │
│                              │ = $0.02/GB for traffic crossing the peer                │
│   Global (cross-region)      │ ~$0.035/GB inbound + $0.035/GB outbound                 │
│                              │ = $0.07/GB for cross-region peering traffic              │
│   Example: 1TB same-region   │ ~$20/month                                               │
│   Example: 1TB cross-region  │ ~$70/month                                               │
│                              │ TIP: Place resources in same region when possible        │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ VPN GATEWAY (S2S / P2S)      │                                                           │
│   Basic SKU                  │ ~$0.04/hour = ~$27/month  (dev only, no SLA)            │
│   VpnGw1                     │ ~$0.19/hour = ~$140/month  (650 Mbps)                   │
│   VpnGw2                     │ ~$0.49/hour = ~$360/month  (1 Gbps)                     │
│   VpnGw3                     │ ~$0.99/hour = ~$730/month  (1.25 Gbps)                  │
│   VpnGw4                     │ ~$1.40/hour = ~$1,020/month (5 Gbps)                    │
│   VpnGw5                     │ ~$2.20/hour = ~$1,600/month (10 Gbps)                   │
│   P2S connections            │ Free up to 128; then ~$0.0075/hr per extra connection   │
│   S2S tunnel                 │ ~$0.05/hour per tunnel (always-on tunnel)               │
│                              │ TIP: Gateway is charged even when idle — use Basic for  │
│                              │      dev and delete when not needed                      │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ EXPRESSROUTE                 │                                                           │
│   Circuit (50 Mbps)          │ ~$55/month (Metered) or ~$165/month (Unlimited)          │
│   Circuit (1 Gbps)           │ ~$350/month (Metered) or ~$1,450/month (Unlimited)       │
│   Circuit (10 Gbps)          │ ~$2,000/month (Metered)                                  │
│   ER Gateway (Standard)      │ ~$0.138/hour = ~$100/month                              │
│   ER Gateway (High Perf)     │ ~$0.384/hour = ~$280/month                              │
│   ER Gateway (Ultra Perf)    │ ~$0.703/hour = ~$510/month                              │
│   Outbound data (Metered)    │ ~$0.025–$0.085/GB depending on zone                     │
│   Outbound data (Unlimited)  │ Included in circuit price                               │
│                              │ TIP: For large data volumes use Unlimited plan          │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ PRIVATE ENDPOINT             │                                                           │
│   PE resource (per hour)     │ ~$0.01/hour = ~$7.30/month per endpoint                 │
│   Data processed (inbound)   │ ~$0.01/GB                                               │
│   Data processed (outbound)  │ ~$0.01/GB                                               │
│   Example: 1 PE + 1TB/month  │ ~$7.30 + $20 = ~$27/month                               │
│                              │ TIP: One PE per sub-resource per service                │
│                              │      (DFS + Blob = 2 PEs for same ADLS account)         │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ PRIVATE DNS ZONE             │                                                           │
│   Zone (per month)           │ ~$0.50/month per zone                                   │
│   DNS queries (after 1B free)│ ~$0.40 per million queries                              │
│   VNet links                 │ Free up to 1000 links per zone                          │
│                              │ TIP: Usually pennies/month; not a cost concern          │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ AZURE FIREWALL               │                                                           │
│   Standard tier (per hour)   │ ~$1.25/hour = ~$912/month                               │
│   Premium tier (per hour)    │ ~$1.52/hour = ~$1,110/month (adds TLS inspection)       │
│   Data processed             │ ~$0.016/GB                                               │
│   Example: Standard + 5TB    │ ~$912 + $80 = ~$992/month                               │
│                              │ TIP: Biggest networking cost in enterprise setups.      │
│                              │      One firewall shared across all spokes saves cost.  │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ APPLICATION GATEWAY          │                                                           │
│   V2 Small                   │ ~$0.008/hour + $0.008/capacity unit                     │
│   V2 Medium (typical)        │ ~$0.25/hour = ~$180/month + capacity units              │
│   WAF V2                     │ ~$0.443/hour = ~$325/month + capacity units             │
│   Data processed             │ ~$0.008/GB                                               │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ LOAD BALANCER                │                                                           │
│   Basic                      │ FREE (but no SLA, being retired)                        │
│   Standard (0-5 rules)       │ ~$0.025/hour = ~$18/month                               │
│   Standard (per rule >5)     │ ~$0.008/hour per additional rule                        │
│   Data processed             │ ~$0.005/GB                                               │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ AZURE BASTION                │                                                           │
│   Basic SKU                  │ ~$0.19/hour = ~$140/month                               │
│   Standard SKU               │ ~$0.49/hour = ~$360/month                               │
│   Outbound data              │ ~$0.12/GB                                               │
│                              │ TIP: Cheaper than public IPs on every VM when you have │
│                              │      many VMs needing admin access.                      │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ DNS PRIVATE RESOLVER         │                                                           │
│   Inbound endpoint           │ ~$0.07/hour = ~$51/month per endpoint                  │
│   Outbound endpoint          │ ~$0.07/hour = ~$51/month per endpoint                  │
│   DNS queries                │ ~$0.70 per million queries                              │
│                              │ TIP: 2 endpoints (in+out) = ~$102/month for hybrid DNS  │
├──────────────────────────────┼─────────────────────────────────────────────────────────┤
│ DATA TRANSFER (GENERAL)      │                                                           │
│   Inbound to Azure           │ FREE                                                    │
│   Outbound Azure → Internet  │ First 100 GB/month free                                 │
│                              │ Then ~$0.087/GB (up to 10 TB)                           │
│                              │ Then ~$0.083/GB (10–50 TB)                              │
│   Within same region         │ FREE (same VNet, same region, no peering)               │
│   Cross-region (Azure zones) │ ~$0.02/GB                                               │
└──────────────────────────────┴─────────────────────────────────────────────────────────┘
```

### Cost summary — free vs paid at a glance
```
COMPLETELY FREE:
  ✅ VNet (create, configure, subnets, route tables)
  ✅ NSG (rules, attachments)
  ✅ Service Endpoints (the feature itself)
  ✅ Inbound data to Azure (upload from on-prem or internet)
  ✅ Traffic within same VNet, same region
  ✅ Azure DNS (default resolver 168.63.129.16)
  ✅ Private DNS Zone queries (first 1B/month)
  ✅ Load Balancer Basic SKU (though retiring)
  ✅ VNet Peering LINK (you pay per GB transferred, not for the link itself)

CHEAP (< $10/month typical use):
  💰 Public IP Standard Static   ~$3.65/month each
  💰 Private DNS Zone            ~$0.50/month per zone
  💰 Private Endpoint            ~$7.30/month + data

MEDIUM ($30–$200/month):
  💰 NAT Gateway                 ~$32/month + $0.045/GB
  💰 VPN Gateway Basic           ~$27/month (dev only)
  💰 VPN Gateway VpnGw1          ~$140/month
  💰 Load Balancer Standard      ~$18/month base
  💰 DNS Private Resolver        ~$102/month (2 endpoints)

EXPENSIVE ($200–$1,000+/month):
  💰💰 VPN Gateway VpnGw2–5     $360–$1,600/month
  💰💰 Azure Bastion Standard   ~$360/month
  💰💰 Application Gateway WAF  ~$325+/month
  💰💰 ExpressRoute circuit      $55–$2,000+/month
  💰💰 ExpressRoute Gateway      $100–$510/month

MOST EXPENSIVE:
  💰💰💰 Azure Firewall Standard  ~$912/month + data
  💰💰💰 Azure Firewall Premium   ~$1,110/month + data
  💰💰💰 ExpressRoute 10 Gbps    ~$2,000+/month
```

### Real-world cost examples

---

#### EXAMPLE 1 — Developer / personal lab (zero budget)
```
Scenario: Learning Azure, one VM, internet access only, no on-prem.

  Component               What you create             Cost
  ──────────────────────  ──────────────────────────  ──────────
  VNet                    vnet-lab (10.0.0.0/16)      FREE
  Subnet                  default (10.0.1.0/24)       FREE
  NSG                     nsg-lab (allow SSH:22)      FREE
  VM NIC                  nic-vm-lab                  FREE
  Public IP               pip-vm-lab (Standard)       ~$3.65/month
  Outbound internet       VM goes directly via pip     FREE (first 100 GB/month)
  ──────────────────────────────────────────────────────────────
  TOTAL NETWORKING COST:                              ~$3.65/month

  Note: You pay for the VM itself (compute) separately.
  The networking overhead for a basic lab is just the Public IP.
```

---

#### EXAMPLE 2 — Small startup web app (2 VMs behind Load Balancer)
```
Scenario: Node.js app on 2 VMs, Standard Load Balancer, no VPN, ADLS for files.

  Component                       Qty  Unit Cost         Monthly Cost
  ──────────────────────────────  ───  ───────────────   ────────────
  VNet + Subnets + NSG            1    FREE              $0
  Standard Load Balancer          1    ~$18/month        $18
  Public IP (on Load Balancer)    1    ~$3.65/month      $3.65
  NAT Gateway (VMs have no PIP)   1    ~$32/month        $32
  NAT Gateway data (50 GB out)    50GB $0.045/GB         $2.25
  Private Endpoint (ADLS)         1    ~$7.30/month      $7.30
  Private Endpoint data (200 GB)  200G $0.01/GB each dir $4
  Private DNS Zone (1 zone)       1    ~$0.50/month      $0.50
  Outbound internet via LB        200G first 100GB free  $8.70
  ──────────────────────────────────────────────────────────────
  TOTAL NETWORKING COST:                                ~$76/month

  Note: VMs themselves cost separately (e.g., 2× B2s = ~$70/month compute).
```

---

#### EXAMPLE 3 — Small dev/test with on-prem VPN access
```
Scenario: Small team, 5 devs need SSH access to Azure VMs.
          1 office needs Site-to-Site VPN for shared access.

  Component                       Qty  Unit Cost         Monthly Cost
  ──────────────────────────────  ───  ───────────────   ────────────
  VNet + Subnets + NSG            1    FREE              $0
  VPN Gateway (Basic SKU)         1    ~$27/month        $27
  S2S VPN tunnel (always-on)      1    ~$36/month        $36
  P2S connections (5 devs)        5    FREE (< 128)      $0
  NAT Gateway                     1    ~$32/month        $32
  NAT data (30 GB outbound)       30GB $0.045/GB         $1.35
  2× Private Endpoints (ADLS+KV)  2    ~$7.30/month each $14.60
  2× Private DNS Zones            2    ~$0.50/month each $1
  ──────────────────────────────────────────────────────────────
  TOTAL NETWORKING COST:                                ~$112/month

  Breakdown of where money goes:
    VPN Gateway + tunnel  = $63/month  (56% of cost)  ← biggest line item
    NAT Gateway           = $33/month  (29%)
    Private Endpoints     = $16/month  (14%)

  Saving tip: Use Basic SKU VPN Gateway for dev. If you only need P2S
  (no site-to-site), skip S2S tunnel cost = save $36/month.
```

---

#### EXAMPLE 4 — Medium production: Databricks + ADLS + SQL + VPN
```
Scenario: Data engineering team. Databricks VNet injected.
          ADLS + SQL via Private Endpoints. VPN to on-prem for source data.
          No Azure Firewall (cost saving decision).

  Component                       Qty  Unit Cost         Monthly Cost
  ──────────────────────────────  ───  ───────────────   ────────────
  VNet + 4 Subnets + NSGs         1    FREE              $0
  VNet Peering (hub↔spoke)        2    $0.02/GB × 2TB   $40
  NAT Gateway (Databricks exit)   1    ~$32/month        $32
  NAT data (Databricks outbound)  100G $0.045/GB         $4.50
  VPN Gateway VpnGw1 (S2S)       1    ~$140/month       $140
  S2S VPN tunnel                  1    ~$36/month        $36
  Private Endpoint: ADLS DFS      1    $7.30/month       $7.30
  Private Endpoint: ADLS Blob     1    $7.30/month       $7.30
  Private Endpoint: Azure SQL     1    $7.30/month       $7.30
  Private Endpoint: Key Vault     1    $7.30/month       $7.30
  PE data transfer (all 4, 2TB)   2TB  $0.01/GB each dir $40
  Private DNS Zones               4    $0.50/month each  $2
  ──────────────────────────────────────────────────────────────
  TOTAL NETWORKING COST:                                ~$323/month

  Breakdown:
    VPN Gateway + tunnel  = $176/month  (55%)  ← on-prem connectivity is expensive
    VNet Peering data     = $40/month   (12%)
    NAT Gateway           = $37/month   (11%)
    Private Endpoints     = $70/month   (22%)  ← 4 PEs + 2TB data

  Note: Databricks cluster compute is separate (much larger cost).
  Network is ~$323/month of a typical ~$2,000–$5,000/month Databricks bill.
```

---

#### EXAMPLE 5 — Web app with WAF + Application Gateway
```
Scenario: Public-facing web app that needs protection from SQL injection / XSS.
          App Gateway WAF in front of 3 backend VMs.

  Component                       Qty  Unit Cost         Monthly Cost
  ──────────────────────────────  ───  ───────────────   ────────────
  VNet + Subnets + NSG            1    FREE              $0
  Application Gateway WAF V2      1    ~$325/month       $325
  App Gateway capacity units      8CU  $0.008/CU/hr×730h $46.72
  Public IP (on App Gateway)      1    ~$3.65/month      $3.65
  Standard Load Balancer          1    ~$18/month        $18
  (internal, between AGW and VMs)
  NAT Gateway (VMs outbound)      1    ~$32/month        $32
  Private Endpoint (SQL DB)       1    ~$7.30/month      $7.30
  Private DNS Zone                1    ~$0.50/month      $0.50
  Inbound data (1 TB from users)  1TB  FREE (inbound)    $0
  Outbound data (500 GB to users) 400G $0 (first 100GB)  
                                  400G $0.087/GB         $34.80
  ──────────────────────────────────────────────────────────────
  TOTAL NETWORKING COST:                                ~$468/month

  Breakdown:
    App Gateway WAF = $372/month  (79%)  ← WAF is the big cost here
    Outbound data   = $35/month   (7%)
    NAT Gateway     = $32/month   (7%)
    Others          = $29/month   (7%)

  Alternative without WAF:
    App Gateway V2 Standard: ~$180/month instead of $325
    Saves ~$145/month but loses WAF protection — not recommended for prod.
```

---

#### EXAMPLE 6 — Remote employees: Azure Bastion vs Public IPs
```
Scenario: 20 VMs in Azure, admins need SSH/RDP access.
          Compare: Public IP on each VM vs one Azure Bastion.

  OPTION A — Public IP on every VM:
    20× Standard Public IPs     20 × $3.65/month      = $73/month
    NSG rules (per VM)          FREE                   = $0
    Risk: 20 attack surfaces exposed to internet       ❌

  OPTION B — Azure Bastion (Standard):
    1× Azure Bastion Standard   $360/month             = $360/month
    0× Public IPs needed        $0                     = $0
    Bastion outbound data       assume 20GB            = $2.40/month
    TOTAL:                                             = ~$362/month
    Benefit: No Public IPs, browser-based access,
             session recording, VNet-peered access     ✅

  OPTION C — Azure Bastion (Basic):
    1× Azure Bastion Basic      $140/month             = $140/month
    0× Public IPs               $0                     = $0
    TOTAL:                                             = ~$140/month
    Limitation: no session recording, no scale-out

  VERDICT for 20 VMs:
    Public IPs:      $73/month  (cheapest but insecure)
    Bastion Basic:   $140/month (secure, 2× cost of public IPs — worth it)
    Bastion Standard:$362/month (for compliance/audit recording needs)

  Crossover point: Bastion Basic becomes cheaper than Public IPs at 38+ VMs
  ($140 / $3.65 per IP = ~38 VMs)
```

---

#### EXAMPLE 7 — On-prem to Azure: VPN vs ExpressRoute cost comparison
```
Scenario: Company transferring 10 TB/month between on-prem and Azure.
          Compare networking cost only.

  OPTION A — VPN Gateway VpnGw2 (S2S):
    VPN Gateway VpnGw2          $360/month
    S2S tunnel                  $36/month
    Data transfer (outbound)    10TB × $0.087/GB = $870/month
    TOTAL:                      ~$1,266/month
    Bandwidth: up to 1 Gbps (shared, variable latency)

  OPTION B — ExpressRoute 1 Gbps Metered:
    ER Circuit (1 Gbps)         $350/month
    ER Gateway (High Perf)      $280/month
    Data outbound (10 TB)       10TB × $0.085/GB = $850/month
    TOTAL:                      ~$1,480/month
    Bandwidth: guaranteed 1 Gbps, consistent low latency, SLA 99.95%

  OPTION C — ExpressRoute 1 Gbps Unlimited:
    ER Circuit (1 Gbps)         $1,450/month
    ER Gateway (High Perf)      $280/month
    Data outbound               INCLUDED
    TOTAL:                      ~$1,730/month
    Bandwidth: guaranteed 1 Gbps, unlimited data

  WHEN DOES EXPRESSROUTE UNLIMITED MAKE SENSE?
    Break-even vs Metered ER:   ($1,450 - $350) / $0.085 = 12.9 TB/month
    If you transfer > 13 TB/month: choose Unlimited (saves on data costs)
    If you transfer < 13 TB/month: Metered is cheaper

  WHEN DOES EXPRESSROUTE MAKE SENSE OVER VPN?
    - VPN is actually CHEAPER for small/medium data volumes
    - ExpressRoute wins when: compliance needs private line, need guaranteed
      bandwidth SLA, need consistent latency (financial trading, real-time)
    - For 10 TB/month: VPN ($1,266) is cheaper than ER Metered ($1,480)
    - ER wins on reliability and latency, not always on price
```

---

#### EXAMPLE 8 — Full Databricks production platform (realistic monthly bill)
```
Scenario: Production Databricks platform.
          2 workspaces (dev + prod), VNet injected, Unity Catalog,
          ADLS Gen2, Key Vault, Azure SQL, VPN to on-prem.
          No ExpressRoute (VPN sufficient), one Azure Firewall.

  NETWORKING COMPONENTS:           MONTHLY COST
  ──────────────────────────────   ────────────
  Hub VNet + 2 Spoke VNets         FREE
  NSGs (all subnets)               FREE
  Service Endpoints                FREE
  Route Tables (UDR)               FREE

  Azure Firewall (Standard)        $912
  Firewall data (3 TB through)     $48    (3,000 GB × $0.016)

  VPN Gateway VpnGw1               $140
  S2S tunnel (always on)           $36
  P2S (10 developers)              FREE   (< 128 connections)

  NAT Gateway (2 workspaces)       $64    (2 × $32)
  NAT data (200 GB outbound)       $9     (200 GB × $0.045)

  VNet Peering (hub↔spoke1+2)      $60    (3TB × $0.02/GB)

  Private Endpoints:
    ADLS DFS (prod + dev)          $14.60 (2 × $7.30)
    ADLS Blob (prod + dev)         $14.60 (2 × $7.30)
    Key Vault (prod + dev)         $14.60 (2 × $7.30)
    Azure SQL                      $7.30
    Databricks UI (prod)           $7.30
  PE data transfer (5 PEs, 2TB)    $40    (2TB × $0.01/GB × 2 dirs)

  Private DNS Zones (5 zones)      $2.50
  DNS Private Resolver             $102   (inbound + outbound endpoints)

  ──────────────────────────────   ────────────
  TOTAL NETWORKING:                ~$1,471/month

  As % of typical Databricks compute bill (~$8,000–$15,000/month):
    Networking = 10–18% of total platform cost

  Top 3 networking costs:
    1. Azure Firewall     $960/month  (65% of networking)
    2. VPN Gateway        $176/month  (12%)
    3. DNS Resolver       $102/month  (7%)

  If you remove Azure Firewall (use NSGs only instead):
    TOTAL drops to ~$559/month — but you lose centralized logging, FQDN filtering
```

### Cost optimization tips
```
💡 Use ONE NAT Gateway per region (shared across multiple subnets) instead of
   one per subnet.

💡 Use ONE Azure Firewall in a hub VNet instead of one per spoke — massive saving.

💡 Delete UNUSED Public IPs and VPN Gateways — they charge even when idle.

💡 For dev/test VPN Gateways: turn them off nights/weekends with Azure Automation
   (delete and recreate, or use Basic SKU and accept variable IPs).

💡 Service Endpoints are FREE and reduce data transfer costs because traffic stays
   on Azure backbone (not metered as internet egress).

💡 Private Endpoints cost ~$7.30/month but they replace the need for expensive
   firewall rules and reduce attack surface — usually worth it.

💡 VNet Peering is cheaper than VPN Gateway for Azure-to-Azure traffic
   (no per-hour gateway cost, only per-GB data).

💡 Global VNet Peering (cross-region) costs ~$0.07/GB vs same-region ~$0.02/GB —
   keep services in the same region when possible.

💡 Inbound data is FREE — pull data into Azure rather than pushing out when designing
   data flows.

💡 ExpressRoute Unlimited plan is better than Metered if you transfer > 5–10 TB/month.
```
