# Azure Networking & Databricks Connectivity — Complete Guide

> **The big picture in one sentence:**
> Azure networking is like a system of private roads and locked buildings inside a city.
> Databricks rents space in that city, your data lives in warehouses (ADLS),
> and every connection between them follows strict road rules — firewalls, private lanes,
> and ID checks at every door.

---

## Table of Contents
1. [Azure Networking Fundamentals](#1-azure-networking-fundamentals)
2. [How Databricks Workspace Connects to Azure](#2-how-databricks-workspace-connects-to-azure)
3. [VNet Injection — Bring Your Own Network](#3-vnet-injection--bring-your-own-network)
4. [Private Link — The Private Tunnel](#4-private-link--the-private-tunnel)
5. [How Unity Catalog Connects](#5-how-unity-catalog-connects)
6. [How Classic Compute (Clusters) Connects](#6-how-classic-compute-clusters-connects)
7. [How Serverless Compute Connects](#7-how-serverless-compute-connects)
8. [Data Flow: Reading ADLS from a Notebook](#8-data-flow-reading-adls-from-a-notebook)
9. [Network Security Layers — All Together](#9-network-security-layers--all-together)
10. [Real-World Architecture Examples](#10-real-world-architecture-examples)
11. [Common Connectivity Problems & Fixes](#11-common-connectivity-problems--fixes)
12. [Quick Reference Cheat Sheet](#12-quick-reference-cheat-sheet)

---

## 1. Azure Networking Fundamentals

Before Databricks, you need to understand Azure's networking building blocks.

### Virtual Network (VNet)
```
Think of a VNet as a PRIVATE OFFICE BUILDING.
- Only people with a key card can enter.
- Everything inside can talk to each other freely.
- Nobody from outside can walk in without going through reception.

Real example:
  VNet: 10.0.0.0/16
  ├── This means IPs from 10.0.0.0 to 10.0.255.255 are "inside" this building
  └── 65,536 possible IP addresses you can assign to resources
```

```
┌─────────────────────────────────────────────────────────┐
│  AZURE SUBSCRIPTION                                       │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │  VNet: 10.0.0.0/16  (your private building)         │ │
│  │                                                       │ │
│  │  ┌─────────────────┐  ┌─────────────────────────┐   │ │
│  │  │ Subnet A        │  │ Subnet B                 │   │ │
│  │  │ 10.0.1.0/24     │  │ 10.0.2.0/24              │   │ │
│  │  │ (floor 1)       │  │ (floor 2)                │   │ │
│  │  │                 │  │                           │   │ │
│  │  │ VM: 10.0.1.4    │  │ DB: 10.0.2.5             │   │ │
│  │  │ VM: 10.0.1.5    │  │ App: 10.0.2.6            │   │ │
│  │  └─────────────────┘  └─────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Subnet
```
A subnet is a FLOOR inside the building.
- Different teams on different floors.
- Floors can have different security rules.
- Resources on the same floor can talk to each other by default.

Example:
  VNet: 10.0.0.0/16
  ├── Subnet: web-tier      10.0.1.0/24   (web servers live here)
  ├── Subnet: data-tier     10.0.2.0/24   (databases live here)
  └── Subnet: databricks    10.0.3.0/24   (Databricks cluster VMs)
```

### Network Security Group (NSG)
```
NSG = SECURITY GUARD at each floor entrance.
Rules decide: who can enter, who can leave, on which door (port).

Example NSG rules for a database subnet:
  INBOUND rules:
  ├── Allow: from web-tier subnet → Port 5432 (PostgreSQL)  ✅
  ├── Allow: from databricks subnet → Port 5432             ✅
  └── Deny:  from Internet → Port 5432                      ❌

  OUTBOUND rules:
  └── Allow: to Internet → Port 443 (HTTPS)                 ✅
```

### CIDR Notation (the /16, /24 numbers)
```
IP: 10.0.0.0/16
     ↑ network  ↑ how many bits are "fixed" (the address of the building)
                  /16 = 16 bits fixed → 16 bits free → 2^16 = 65,536 IPs
                  /24 = 24 bits fixed → 8 bits free  → 2^8  = 256 IPs
                  /26 = 26 bits fixed → 6 bits free  → 2^6  = 64 IPs

Common sizes:
  /8   → 16 million IPs  (huge — entire company)
  /16  → 65,536 IPs      (VNet level)
  /24  → 256 IPs         (subnet level — typical)
  /26  → 64 IPs          (small subnet — enough for Databricks)
  /28  → 16 IPs          (tiny — just a few VMs)
```

### Service Endpoint vs Private Endpoint vs Private Link

These are three ways a resource inside your VNet can securely talk to an Azure service (like ADLS, Key Vault, SQL):

```
┌─────────────────────────────────────────────────────────────────────┐
│                     THREE CONNECTION METHODS                         │
├──────────────────┬──────────────────────────┬───────────────────────┤
│ Method           │ Traffic Route             │ IP Type               │
├──────────────────┼──────────────────────────┼───────────────────────┤
│ Public (default) │ Goes through Internet     │ Public IP             │
│                  │ (leaves Azure backbone)   │ of the Azure service  │
├──────────────────┼──────────────────────────┼───────────────────────┤
│ Service Endpoint │ Stays on Azure backbone   │ Still public IP but   │
│                  │ (doesn't touch Internet)  │ restricted to VNet    │
├──────────────────┼──────────────────────────┼───────────────────────┤
│ Private Endpoint │ Stays FULLY inside VNet   │ Private IP from YOUR  │
│ (Private Link)   │ Never touches Internet    │ subnet (e.g.10.0.2.9) │
└──────────────────┴──────────────────────────┴───────────────────────┘

Analogy:
  Public endpoint   = walking to a store through city streets (visible to everyone)
  Service endpoint  = a private road to the store (less visible but store is still "public")
  Private endpoint  = the store opens a door directly into your building (fully private)
```

---

## 2. How Databricks Workspace Connects to Azure

### The Two-Plane Architecture

Databricks operates in TWO planes — and this is the key to understanding all networking:

```
┌───────────────────────────────────────────────────────────────────┐
│                    DATABRICKS ARCHITECTURE                          │
├───────────────────────────┬───────────────────────────────────────┤
│   CONTROL PLANE           │   DATA PLANE                           │
│   (Databricks-managed)    │   (Your Azure Subscription)            │
├───────────────────────────┼───────────────────────────────────────┤
│ • Web UI (notebooks, UI)  │ • Cluster VMs (actual compute)         │
│ • Job scheduler           │ • VNet (your network)                  │
│ • Cluster manager         │ • Storage (ADLS Gen2)                  │
│ • REST API endpoints      │ • Metastore (Unity Catalog)            │
│ • Unity Catalog service   │ • DBFS root storage                    │
│ • Hive metastore          │                                        │
│                           │                                        │
│ Runs IN: Databricks'      │ Runs IN: YOUR Azure subscription       │
│ Azure subscription        │ YOUR Azure region                      │
│ (you don't see this)      │ (you control this)                     │
└───────────────────────────┴───────────────────────────────────────┘
         ↑                                ↑
         │    Secure channel (HTTPS/443)  │
         └────────────────────────────────┘
         Control plane talks to data plane
         to create/manage clusters
```

### What Happens When You Create a Workspace

```
Step 1: You create a Databricks workspace in Azure Portal
        ↓
Step 2: Azure creates a "managed resource group" in YOUR subscription
        e.g.: rg-adb-4567890123456789  (you can see this but shouldn't touch it)
        Inside it:
        ├── Azure Databricks VNet (if no-VNet-injection)
        ├── 2 subnets (private + public)
        ├── NSG
        └── Storage account (DBFS root)
        ↓
Step 3: Databricks control plane gets a secure channel into your managed RG
        ↓
Step 4: When you create a cluster, the control plane launches VMs
        inside your managed RG's VNet
```

### Default Networking (No VNet Injection)

```
┌────────────────────────────────────────────────────────────────────┐
│ YOUR AZURE SUBSCRIPTION                                              │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Managed Resource Group (auto-created, databricks-managed)   │   │
│  │ rg-adb-4567890123456789                                      │   │
│  │                                                               │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │ Managed VNet: 10.139.0.0/16  (databricks owns this) │    │   │
│  │  │                                                       │    │   │
│  │  │  ┌──────────────────┐  ┌──────────────────────────┐ │    │   │
│  │  │  │ Public Subnet    │  │ Private Subnet            │ │    │   │
│  │  │  │ 10.139.0.0/18    │  │ 10.139.64.0/18            │ │    │   │
│  │  │  │                  │  │                            │ │    │   │
│  │  │  │ Cluster driver   │  │ Cluster workers            │ │    │   │
│  │  │  │ VM               │  │ VMs                        │ │    │   │
│  │  │  └──────────────────┘  └──────────────────────────┘ │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌────────────────────┐                                             │
│  │ YOUR Resource Group │  ← your ADLS, Key Vault, SQL live here     │
│  │ (you control this) │                                             │
│  └────────────────────┘                                             │
└────────────────────────────────────────────────────────────────────┘

Problem with default networking:
- The managed VNet is separate from YOUR VNet
- Cluster VMs can't privately reach YOUR ADLS or SQL
- Traffic between clusters and your data goes over public internet (or service endpoints)
```

---

## 3. VNet Injection — Bring Your Own Network

VNet Injection solves the default networking problem by putting Databricks cluster VMs directly into YOUR VNet.

### What VNet Injection Does

```
WITHOUT VNet Injection:               WITH VNet Injection:
  Databricks VNet ←→ Internet         YOUR VNet
  YOUR VNet       ←→ Internet         ├── your-subnet-1 (web apps)
  (two separate bubbles)              ├── your-subnet-2 (databases)
                                      ├── dbr-public-subnet   ← clusters go HERE
                                      └── dbr-private-subnet  ← clusters go HERE

Result: Cluster VMs are now INSIDE your network.
        They can reach your ADLS, SQL Server, Key Vault privately.
        No traffic leaves your VNet.
```

### Requirements for VNet Injection

```
Your VNet must have:
├── At least 2 dedicated subnets (cannot be shared with other resources):
│   ├── Databricks Public Subnet  (e.g. 10.0.3.0/26  — min /26 = 64 IPs)
│   └── Databricks Private Subnet (e.g. 10.0.4.0/26  — min /26 = 64 IPs)
│
├── NSG attached to BOTH subnets with specific Databricks-required rules
│   (Azure portal auto-adds these if you use the guided setup)
│
└── Microsoft.Databricks service delegation on both subnets
    (tells Azure: "these subnets are reserved for Databricks")
```

### VNet Injection Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│ YOUR AZURE SUBSCRIPTION                                                  │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ YOUR VNet: 10.0.0.0/16                                           │   │
│  │                                                                   │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │   │
│  │  │ app-subnet   │  │ data-subnet  │  │ dbr-public-subnet    │  │   │
│  │  │ 10.0.1.0/24  │  │ 10.0.2.0/24  │  │ 10.0.3.0/26          │  │   │
│  │  │              │  │              │  │ (cluster driver VMs) │  │   │
│  │  │ Your APIs    │  │ SQL Server   │  └──────────────────────┘  │   │
│  │  │ Your Apps    │  │ ADLS PE      │                              │   │
│  │  └──────────────┘  └──────────────┘  ┌──────────────────────┐  │   │
│  │          ↑ private access ↑           │ dbr-private-subnet   │  │   │
│  │                                       │ 10.0.4.0/26          │  │   │
│  │                                       │ (cluster worker VMs) │  │   │
│  │                                       └──────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  Cluster VMs (in dbr-private-subnet) can now privately reach:           │
│  ✅ SQL Server in data-subnet  (private IP, no internet)                │
│  ✅ ADLS via Private Endpoint  (private IP, no internet)                │
│  ✅ Key Vault via Private Link  (private IP, no internet)               │
└────────────────────────────────────────────────────────────────────────┘
```

### Concrete Example — VNet Injection Setup

```
Scenario: Company wants clusters to read from ADLS Gen2 securely

Step 1: Create VNet
  Name: vnet-databricks-prod
  Address space: 10.0.0.0/16
  Region: East US

Step 2: Create subnets
  Subnet 1: dbr-public    10.0.3.0/26   (delegate to Microsoft.Databricks/workspaces)
  Subnet 2: dbr-private   10.0.4.0/26   (delegate to Microsoft.Databricks/workspaces)

Step 3: Create NSG and attach to both subnets
  NSG rule (inbound): Allow Databricks control plane IPs to reach clusters
  (Azure adds these automatically if you use the ARM template)

Step 4: Create Databricks workspace
  Custom VNet: vnet-databricks-prod
  Public subnet:  dbr-public  (10.0.3.0/26)
  Private subnet: dbr-private (10.0.4.0/26)

Step 5: Create Private Endpoint for ADLS
  Storage account: adlsprod
  Private endpoint: pe-adls-prod
  Subnet: data-subnet (10.0.2.0/24)
  Private IP assigned: 10.0.2.9

Step 6: Create Private DNS Zone
  Zone: privatelink.dfs.core.windows.net
  Record: adlsprod → 10.0.2.9
  (When cluster resolves adlsprod.dfs.core.windows.net, DNS returns 10.0.2.9 not public IP)

Result:
  Cluster (10.0.4.5) → DNS lookup → 10.0.2.9 → ADLS Private Endpoint
  Traffic never leaves your VNet. No public IP involved.
```

---

## 4. Private Link — The Private Tunnel

Private Link puts Azure services (ADLS, Key Vault, SQL, even the Databricks UI) directly inside your VNet with a private IP.

### How Private Link Works

```
WITHOUT Private Link:
  Your cluster (10.0.4.5)
    → DNS: adlsprod.dfs.core.windows.net → 20.150.x.x  (PUBLIC IP)
    → Traffic goes: VNet → Azure backbone → Public endpoint of ADLS
    → ADLS firewall must allow Azure IPs (wide)

WITH Private Link (Private Endpoint):
  Azure creates a Network Interface Card (NIC) in YOUR subnet
  with a PRIVATE IP: 10.0.2.9
  That NIC is connected directly to adlsprod

  Your cluster (10.0.4.5)
    → DNS: adlsprod.dfs.core.windows.net → 10.0.2.9  (PRIVATE IP — from your VNet)
    → Traffic goes: VNet → 10.0.2.9 NIC → ADLS (never leaves your VNet)
    → ADLS firewall can deny ALL public access
```

### Private Link for Databricks Workspace Itself

```
Problem: When you open the Databricks UI in your browser, it goes to:
  https://adb-1234567890.azuredatabricks.net
  That's a PUBLIC endpoint → traffic goes through internet

Solution: Private Link for the workspace (also called "front-end private link")

  Creates a private endpoint in YOUR VNet with a private IP (e.g. 10.0.5.10)
  Points to the Databricks control plane

  With Private DNS Zone override:
  adb-1234567890.azuredatabricks.net → 10.0.5.10 (inside your VNet)

  Now even the NOTEBOOK UI is accessed through your private network.
  No traffic to/from Databricks UI touches the internet.
```

### Private Link Architecture for Full Isolation

```
┌─────────────────────────────────────────────────────────────────────┐
│ YOUR VNet: 10.0.0.0/16                                               │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ pe-subnet: 10.0.5.0/24  (Private Endpoints live here)       │    │
│  │                                                               │    │
│  │  ┌─────────────────────────────────────────────────────┐    │    │
│  │  │ Private Endpoint NIC: pe-databricks  → 10.0.5.10   │    │    │
│  │  │   Points to: Databricks workspace front-end          │    │    │
│  │  │                                                       │    │    │
│  │  │ Private Endpoint NIC: pe-adls        → 10.0.5.11   │    │    │
│  │  │   Points to: ADLS Gen2 (DFS endpoint)                │    │    │
│  │  │                                                       │    │    │
│  │  │ Private Endpoint NIC: pe-keyvault    → 10.0.5.12   │    │    │
│  │  │   Points to: Azure Key Vault          │    │    │
│  │  │                                                       │    │    │
│  │  │ Private Endpoint NIC: pe-sql         → 10.0.5.13   │    │    │
│  │  │   Points to: Azure SQL / Synapse      │    │    │
│  │  └─────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  DNS Resolution (Private DNS Zones linked to this VNet):             │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ privatelink.azuredatabricks.net  → 10.0.5.10                 │   │
│  │ privatelink.dfs.core.windows.net → 10.0.5.11                 │   │
│  │ privatelink.vaultcore.azure.net  → 10.0.5.12                 │   │
│  │ privatelink.database.windows.net → 10.0.5.13                 │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 5. How Unity Catalog Connects

Unity Catalog (UC) is the governance layer. It has its own networking story.

### Unity Catalog Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     UNITY CATALOG COMPONENTS                         │
├───────────────────────────┬─────────────────────────────────────────┤
│ Metastore (control plane) │ External Locations (data plane)          │
│ Databricks-managed        │ YOUR Azure subscription                  │
├───────────────────────────┼─────────────────────────────────────────┤
│ • Table/schema/catalog    │ • ADLS Gen2 storage containers            │
│   metadata                │ • Managed tables: root storage            │
│ • Permission grants       │ • External tables: your ADLS paths        │
│ • Column-level security   │ • Volumes: files on ADLS                  │
│ • Audit logs              │                                           │
│                           │ Access controlled by:                     │
│ One metastore per         │ • Storage Credentials (Service Principal  │
│ Azure region per          │   or Managed Identity)                    │
│ Databricks account        │ • External Location definitions           │
└───────────────────────────┴─────────────────────────────────────────┘
```

### Storage Credential — How UC Proves Identity to ADLS

```
Problem: UC needs to read/write to your ADLS containers.
         ADLS needs to know WHO is asking.

Solution: Storage Credential — a registered identity in Databricks.

Two types:
  1. Service Principal + Client Secret/Certificate
  2. Managed Identity (recommended — no passwords!)
```

```
MANAGED IDENTITY FLOW:

Step 1: Create a User-Assigned Managed Identity in Azure
        Name: mi-databricks-uc
        Azure assigns it an Object ID: abc-123-def

Step 2: Grant it Storage Blob Data Contributor on your ADLS
        Scope: Storage Account or specific container

Step 3: Assign the Managed Identity to the Databricks Access Connector
        (Access Connector is an Azure resource that lets Databricks assume a Managed Identity)
        az databricks access-connector create \
          --name adb-connector-prod \
          --resource-group rg-databricks \
          --identity-type UserAssigned \
          --user-assigned-identity /subscriptions/.../mi-databricks-uc

Step 4: Register in Unity Catalog as a Storage Credential
        CREATE STORAGE CREDENTIAL my_adls_credential
          WITH AZURE_MANAGED_IDENTITY (
            connector_id = '/subscriptions/.../adb-connector-prod'
          );

Step 5: Create External Location using the credential
        CREATE EXTERNAL LOCATION my_datalake
          URL 'abfss://silver@adlsprod.dfs.core.windows.net/'
          WITH (STORAGE CREDENTIAL my_adls_credential);

Step 6: Grant table access (not storage access — UC handles translation)
        GRANT SELECT ON TABLE catalog.schema.table TO `data_analyst@company.com`;

When a user runs SELECT:
  User identity → UC checks: does this user have SELECT?
  Yes → UC uses the Storage Credential (Managed Identity) to fetch data from ADLS
  User NEVER sees the ADLS URL or has direct storage access
```

### Unity Catalog Network Flow

```
User runs: SELECT * FROM catalog.silver.transactions

                    ┌────────────────────────────────┐
                    │  Databricks Control Plane       │
                    │  ┌─────────────────────────┐   │
  1. Query hits ───→│  │  Unity Catalog Service  │   │
     UC endpoint    │  │                          │   │
                    │  │  Check: does user have   │   │
                    │  │  SELECT on this table?   │   │
                    │  │  Yes → resolve location  │   │
                    │  │  abfss://silver@adls...  │   │
                    │  └─────────────────────────┘   │
                    └────────────────────────────────┘
                                    │
                    2. Sends execution plan to cluster
                                    │
                                    ▼
                    ┌────────────────────────────────┐
                    │  Your Cluster (Data Plane)      │
                    │  (in your VNet subnet)           │
                    │                                  │
                    │  3. Cluster assumes Managed      │
                    │     Identity via Access Connector│
                    │                                  │
                    │  4. Requests OAuth token from    │
                    │     Azure AD for the MI          │
                    └────────────────────────────────┘
                                    │
                    5. Bearer token in Authorization header
                                    │
                                    ▼
                    ┌────────────────────────────────┐
                    │  ADLS Gen2                      │
                    │  Private Endpoint: 10.0.5.11    │
                    │                                  │
                    │  6. Validates token: MI has      │
                    │     Storage Blob Data Reader     │
                    │     on this container? ✅        │
                    │                                  │
                    │  7. Returns data to cluster      │
                    └────────────────────────────────┘
```

### Unity Catalog Metastore Region Requirement

```
⚠️ IMPORTANT: Unity Catalog metastore and workspaces must be in the SAME Azure region.

Metastore: East US
  ├── Workspace A: East US   ✅ can attach
  ├── Workspace B: East US   ✅ can attach
  └── Workspace C: West US   ❌ CANNOT attach (different region)

Root Storage:
  The metastore has a root ADLS container for MANAGED tables.
  This ADLS must also be in the SAME region.
  e.g.: adlsprodeastus.dfs.core.windows.net/uc-root/

External locations can point to ADLS in any region but performance suffers.
```

---

## 6. How Classic Compute (Clusters) Connects

When you create a cluster (interactive or job compute), here's what happens:

### Cluster Startup Network Flow

```
You click "Create Cluster" or a job triggers cluster start

Step 1: CONTROL PLANE → DATA PLANE
  Databricks control plane sends a request to Azure Resource Manager (ARM)
  in YOUR subscription to create VMs in your VNet (if VNet injection is used)

Step 2: VM CREATION
  Azure creates N VMs (1 driver + N-1 workers) in your dbr-private-subnet
  Each VM gets a private IP from your subnet range:
    Driver:  10.0.4.4
    Worker1: 10.0.4.5
    Worker2: 10.0.4.6
    Worker3: 10.0.4.7

Step 3: DATABRICKS AGENT
  Each VM downloads the Databricks agent (Spark, JVM, Python, etc.)
  The VM must be able to reach:
    ├── Azure Storage (to download agent binaries)  → via Service Endpoint or NAT
    ├── Databricks control plane  (HTTPS 443)       → via NAT Gateway or Public IP
    └── Other cluster VMs  (Spark shuffle, etc.)    → within VNet (private)

Step 4: SPARK STARTUP
  Driver VM starts the Spark master
  Worker VMs connect to driver (within VNet, private)
  Cluster is READY

Step 5: READING DATA (e.g. from ADLS)
  Cluster VMs → DNS resolution → ADLS private endpoint (10.0.5.11)
  → Bearer token from Azure AD (OAuth)
  → Data flows back through private endpoint
  → Spark distributes data across worker VMs
```

### Cluster Internet Access Options

```
Clusters need some outbound internet access (to download packages, reach APIs).
Three options for how outbound traffic works:

OPTION 1: Public Subnet (default, not recommended for prod)
  Driver VM has a Public IP
  Traffic goes directly to internet via the Public IP
  ❌ Your cluster IP is exposed to internet

OPTION 2: No Public IP (NPIP) — recommended
  VMs have NO public IP
  Outbound traffic goes through:
    ├── Azure NAT Gateway (recommended) — one shared outbound IP
    └── OR User-Defined Route (UDR) to a firewall (e.g. Azure Firewall)
  ✅ Cluster IPs are never exposed

OPTION 3: Customer-Managed NAT/Firewall
  All outbound traffic routes through YOUR firewall
  You control what the cluster can access
  Required ports:
    443  (HTTPS) to: Databricks control plane, Azure services
    3306 (MySQL-like) to: Hive metastore if using external
```

### What Inbound Ports Do Clusters Need?

```
NSG Rules required on Databricks subnets:

INBOUND to public subnet:
  ├── Allow: VirtualNetwork → VirtualNetwork → Any (cluster ↔ cluster)
  └── Allow: Databricks control plane IPs → 443 (management channel)

INBOUND to private subnet:
  └── Allow: VirtualNetwork → VirtualNetwork → Any (cluster ↔ cluster)

OUTBOUND from both subnets:
  ├── Allow: to VirtualNetwork → Any  (cluster ↔ cluster, cluster → ADLS PE)
  ├── Allow: to AzureDatabricks service tag → 443 (control plane communication)
  ├── Allow: to AzureStorage → 443 (download binaries, DBFS)
  └── Allow: to Sql → 1433 (optional — only if cluster access to SQL Server)
```

---

## 7. How Serverless Compute Connects

Serverless SQL Warehouses and Serverless Jobs compute are architecturally very different from classic clusters.

### What is Serverless?

```
CLASSIC COMPUTE:                        SERVERLESS:
  VMs launch in YOUR VNet               Compute runs in DATABRICKS'S VNet
  You wait 3-7 min for startup          Starts in seconds (pre-warmed)
  You pay when cluster is idle          Pay only when query runs
  You manage VNet injection             Databricks manages the infrastructure
  Compute stays in YOUR subscription    Compute in Databricks subscription
```

### The Networking Challenge with Serverless

```
PROBLEM:
  Serverless compute runs in Databricks' own Azure subscription/VNet.
  Your data is in YOUR Azure subscription (ADLS, SQL Server, etc.).
  How does serverless compute reach YOUR data?

ANSWER: Network Connectivity Configuration (NCC) / Private Connectivity

Two approaches:

  1. OUTBOUND: Serverless compute reaches YOUR Private Endpoints
     via Stable Outbound IPs → your ADLS/SQL firewall allowlist

  2. INBOUND: You create Private Endpoints that point INTO the
     Serverless compute VNet (Preview feature)
```

### Serverless Stable Outbound IPs (Simple Approach)

```
Scenario: Serverless SQL Warehouse needs to read from your ADLS Gen2.

ADLS has a firewall: only allow specific IP ranges.

Databricks publishes STABLE outbound IP ranges for serverless per region.
For East US: e.g., 20.49.x.x/28  (small fixed range)

You allowlist these IPs in your ADLS firewall:
  ADLS Storage Account → Networking → Firewall
  → Add IP range: 20.49.x.x/28

Now serverless can reach your ADLS.

Limitations:
  ❌ Not truly private (traffic touches public internet/Azure backbone)
  ❌ IP ranges are shared across all Databricks customers in the region
  ✅ Simple to set up
  ✅ No VNet management needed
```

### Serverless Private Connectivity (NCC — recommended for prod)

```
Network Connectivity Configuration (NCC):
  A Databricks account-level object that links serverless compute to
  Private Endpoints in YOUR VNet.

┌─────────────────────────────────────────────────────────────────────┐
│ DATABRICKS SERVERLESS VNet                                           │
│ (in Databricks' Azure subscription — you don't control this)         │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Serverless Compute Nodes                                      │    │
│  │ (pre-warmed, shared across customers, isolated per customer) │    │
│  │                                                               │    │
│  │  NCC Configuration:                                           │    │
│  │  → Route traffic for adlsprod.dfs.core.windows.net            │    │
│  │    through Private Endpoint ID: /subscriptions/.../pe-adls    │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                           │ Private connection                        │
└───────────────────────────│─────────────────────────────────────────┘
                            │
          ┌─────────────────▼──────────────────────────────────────────┐
          │ YOUR Azure Subscription                                       │
          │                                                               │
          │  Private Endpoint: pe-adls-serverless                         │
          │  Subnet: 10.0.5.0/24                                          │
          │  Private IP: 10.0.5.20                                         │
          │  → Connected to: adlsprod.dfs.core.windows.net                │
          │                                                               │
          │  ADLS Gen2: adlsprod                                           │
          │  Firewall: deny all public + allow private endpoint only ✅    │
          └───────────────────────────────────────────────────────────────┘
```

### Setting Up NCC (Serverless Private Connectivity)

```
Step 1: Create Private Endpoint for ADLS in YOUR subscription
  - Target: adlsprod storage account, DFS sub-resource
  - Subnet: your pe-subnet
  - Gets approved: either auto (same subscription) or manual

Step 2: Create NCC in Databricks Account Console
  az databricks network-connectivity-config create \
    --name ncc-prod-eastus \
    --region eastus

Step 3: Add Private Endpoint Rule to NCC
  # Tell Databricks: "route ADLS traffic through this PE"
  Databricks UI: Account → Network → NCC → Add rule
  Rule type: Azure Private Link
  Resource: /subscriptions/.../adlsprod (storage account)
  Sub-resource: dfs

  Databricks creates a "Private Endpoint Connection Request" on your ADLS.
  You must APPROVE it in Azure Portal:
  Storage Account → Networking → Private Endpoint Connections → Approve

Step 4: Attach NCC to Workspace
  Databricks UI: Workspace Settings → Network → Serverless NCC: ncc-prod-eastus

Result:
  All serverless SQL Warehouse queries hitting adlsprod go through private endpoint.
  ADLS firewall can block everything except that private endpoint. ✅
```

### Serverless vs Classic — Networking Comparison

```
┌─────────────────────────┬──────────────────────────┬───────────────────────────┐
│ Aspect                  │ Classic Compute           │ Serverless                │
├─────────────────────────┼──────────────────────────┼───────────────────────────┤
│ Where compute runs      │ YOUR VNet (with injection)│ Databricks' VNet          │
│ Startup time            │ 3–10 minutes              │ Seconds                   │
│ Network control         │ Full (NSG, UDR, firewall) │ Limited (NCC/IP allowlist)│
│ Private data access     │ Via Private Endpoint in   │ Via NCC + PE approval     │
│                         │ your subnet               │ or Stable Outbound IPs    │
│ ADLS Private Endpoint   │ PE in your subnet,        │ PE in your subnet,        │
│                         │ cluster resolves via DNS  │ NCC routes traffic to it  │
│ Cost when idle          │ Yes (VM hours)            │ No (pay per query)        │
│ Internet egress option  │ NAT Gateway / Azure FW    │ Databricks managed        │
│ VNet Injection needed?  │ Yes (for private access)  │ No                        │
└─────────────────────────┴──────────────────────────┴───────────────────────────┘
```

---

## 8. Data Flow: Reading ADLS from a Notebook

Let's trace every single hop when a notebook runs `spark.read.parquet("abfss://silver@adlsprod.dfs.core.windows.net/transactions/")`.

```
You type: spark.read.parquet("abfss://silver@adlsprod.dfs.core.windows.net/")

──────────────────────────────────────────────────────────────────────
HOP 1: Browser → Databricks Control Plane
  Your browser (on corp laptop, VPN connected to your VNet)
  → HTTPS request to adb-12345.azuredatabricks.net
  → If Private Link: resolves to 10.0.5.10 (private IP in your VNet)
  → Notebook command sent to control plane

──────────────────────────────────────────────────────────────────────
HOP 2: Control Plane → Cluster Driver VM
  Control plane sends Spark command to driver VM (10.0.4.4)
  via secure WebSocket connection (port 443)
  Driver is in YOUR VNet → connection goes through Private Link or
  the Relay connection that Databricks maintains

──────────────────────────────────────────────────────────────────────
HOP 3: Unity Catalog Permission Check
  Driver: "Before reading, UC must authorize this."
  Driver calls UC service (control plane): "Can user X read this path?"
  UC checks: user X → has SELECT on catalog.silver.transactions? ✅
  UC returns: authorized, use storage credential 'my_adls_cred'

──────────────────────────────────────────────────────────────────────
HOP 4: Driver Gets OAuth Token for ADLS
  Driver → Azure AD (login.microsoftonline.com): "Give me a token for
  Managed Identity: mi-databricks-uc"
  Azure AD: validates the MI, returns Bearer token (JWT, valid 1 hour)

  This call goes to: login.microsoftonline.com
  Via NAT Gateway (if No Public IP) or Direct (if public subnet)

──────────────────────────────────────────────────────────────────────
HOP 5: List Files (DNS → Private Endpoint → ADLS)
  Driver: resolve adlsprod.dfs.core.windows.net
  DNS (Azure Private DNS Zone): returns 10.0.5.11 (Private Endpoint IP)
  NOT the public IP 20.150.x.x

  Driver → 10.0.5.11:443 (inside your VNet, private)
  Request: GET /silver/transactions?list
  Header: Authorization: Bearer eyJ... (the token from step 4)
  ADLS: validates token, returns file list (parquet files)

──────────────────────────────────────────────────────────────────────
HOP 6: Driver Plans Spark Job
  Driver examines parquet metadata, creates Spark execution plan
  Decides: 4 tasks (one per parquet file partition)
  Assigns tasks to workers: W1=task1, W2=task2, W3=task3, W4=task4

──────────────────────────────────────────────────────────────────────
HOP 7: Workers Read Data in Parallel
  Worker1 (10.0.4.5) → 10.0.5.11:443 → GET /silver/transactions/part-0001.parquet
  Worker2 (10.0.4.6) → 10.0.5.11:443 → GET /silver/transactions/part-0002.parquet
  Worker3 (10.0.4.7) → 10.0.5.11:443 → GET /silver/transactions/part-0003.parquet
  Worker4 (10.0.4.8) → 10.0.5.11:443 → GET /silver/transactions/part-0004.parquet

  All 4 reads happen simultaneously.
  Each worker caches its partition in JVM memory.

──────────────────────────────────────────────────────────────────────
HOP 8: Driver Collects (if action like .count() or .show())
  Workers → Driver (within VNet, private: 10.0.4.x → 10.0.4.4)
  Driver aggregates and returns result

──────────────────────────────────────────────────────────────────────
HOP 9: Result → Browser
  Driver → Control plane (via Relay connection)
  Control plane → Your browser (HTTPS)
  Result displayed in notebook cell output
```

---

## 9. Network Security Layers — All Together

Multiple layers of security work together. Here's the full stack:

```
LAYER 1: Azure AD Authentication
  Every request must carry a valid token.
  "Who are you?"
  Service Principals, Managed Identities, User accounts.

LAYER 2: Azure RBAC (Role-Based Access Control)
  "What Azure resources are you allowed to control?"
  e.g., Storage Blob Data Reader on the ADLS account.
  Applied at Azure subscription level.

LAYER 3: Unity Catalog Permissions
  "What DATA are you allowed to read/write?"
  e.g., GRANT SELECT ON TABLE catalog.silver.tx TO analyst@company.com
  Applied inside Databricks — independent of Azure RBAC.

LAYER 4: Network Security Groups (NSG)
  "Which IP:port combinations are allowed into/out of this subnet?"
  e.g., Block all inbound from Internet except specific ports.
  Applied at subnet or NIC level.

LAYER 5: Storage Account Firewall
  "Which networks/IPs can reach this storage account?"
  e.g., Allow only from: Private Endpoint subnet + NCC outbound IPs.
  Deny public access: enabled.

LAYER 6: Private Endpoints
  "Are you even able to see the service's IP?"
  If not in the same VNet or peered VNet, you can't even route to it.
  DNS override ensures private IP is returned, not public.

LAYER 7: Azure Firewall / NVA (optional, enterprise)
  "Inspect all traffic leaving the VNet."
  Log, allow, or block based on FQDN rules.
  e.g., Allow: *.dfs.core.windows.net | Block: *.pastebin.com
```

---

## 10. Real-World Architecture Examples

### Example A — Basic Secure Setup (Dev/Test)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Resource Group: rg-databricks-dev                                    │
│ Region: East US                                                       │
│                                                                       │
│  VNet: vnet-dev (10.10.0.0/16)                                       │
│  ├── Subnet: dbr-public   (10.10.1.0/26)  — cluster drivers          │
│  ├── Subnet: dbr-private  (10.10.2.0/26)  — cluster workers          │
│  └── Subnet: pe-subnet    (10.10.3.0/24)  — private endpoints        │
│                                                                       │
│  Resources:                                                           │
│  ├── Databricks Workspace (VNet injected into dbr subnets)            │
│  ├── ADLS Gen2: adlsdev                                               │
│  │   └── Private Endpoint: pe-adls → 10.10.3.5                       │
│  └── Key Vault: kv-dev                                                │
│      └── Private Endpoint: pe-kv → 10.10.3.6                         │
│                                                                       │
│  DNS Zones (linked to vnet-dev):                                      │
│  ├── privatelink.dfs.core.windows.net → 10.10.3.5                    │
│  └── privatelink.vaultcore.azure.net  → 10.10.3.6                    │
│                                                                       │
│  NAT Gateway: nat-dev                                                  │
│  └── Static outbound IP: 52.x.x.x (used by clusters to reach internet)│
└─────────────────────────────────────────────────────────────────────┘
```

### Example B — Production Enterprise Setup

```
┌─────────────────────────────────────────────────────────────────────┐
│ Azure Subscription: sub-prod-data-platform                           │
│ Region: East US                                                       │
│                                                                       │
│  Hub VNet: vnet-hub (10.0.0.0/16)  ←── Corp network via VPN/ExpressRoute
│  ├── Azure Firewall Subnet: 10.0.0.0/26                              │
│  └── VPN Gateway Subnet: 10.0.1.0/26                                 │
│                                                                       │
│  Spoke VNet: vnet-databricks (10.1.0.0/16)                           │
│  Peered with Hub VNet                                                 │
│  ├── dbr-public-subnet    10.1.1.0/26   (cluster drivers)            │
│  ├── dbr-private-subnet   10.1.2.0/26   (cluster workers)            │
│  └── pe-subnet            10.1.3.0/24   (all private endpoints)      │
│                                                                       │
│  All outbound traffic → UDR → Azure Firewall (in hub)                │
│  Azure Firewall FQDN rules:                                           │
│  Allow: *.azuredatabricks.net, *.dfs.core.windows.net                │
│  Allow: *.vault.azure.com, pypi.org, *.anaconda.com                  │
│  Deny: everything else                                                │
│                                                                       │
│  Private Endpoints (in 10.1.3.0/24):                                 │
│  ├── pe-databricks-ui → Databricks workspace front-end               │
│  ├── pe-adls-silver   → adlsprod (silver container)                  │
│  ├── pe-adls-bronze   → adlsprod (bronze container)                  │
│  ├── pe-keyvault      → kv-databricks-prod                           │
│  └── pe-sql           → sqlserver-prod.database.windows.net          │
│                                                                       │
│  Unity Catalog:                                                       │
│  ├── Metastore: uc-metastore-eastus                                   │
│  ├── Root ADLS: adlsucroot/metastore/                                 │
│  ├── Access Connector: adb-connector-prod                             │
│  └── Managed Identity: mi-databricks-uc                               │
│      └── Role: Storage Blob Data Contributor on adlsprod              │
│                                                                       │
│  Serverless (SQL Warehouses):                                         │
│  └── NCC: ncc-prod-eastus                                             │
│      └── PE Rule: adlsprod DFS → pe-adls-serverless (10.1.3.50)      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 11. Common Connectivity Problems & Fixes

### Problem 1 — "Cannot read from ADLS — 403 Forbidden"

```
Symptom: SparkException: 403 This request is not authorized to perform this operation.

Likely causes and fixes:

CAUSE A: Storage account firewall is blocking the cluster IP
  Check: Storage Account → Networking → Firewall
  Fix: Add the cluster's subnet to the allowed VNet rules
       OR: Set up Private Endpoint

CAUSE B: Managed Identity / Service Principal doesn't have the right role
  Check: Storage Account → Access Control (IAM) → Role Assignments
  Fix: Add role "Storage Blob Data Contributor" to the MI or SP
       on the storage account OR specific container

CAUSE C: Unity Catalog External Location URL doesn't match
  Check: SHOW EXTERNAL LOCATIONS; — see what URL is registered
  Fix: The notebook path must START WITH the registered external location URL
       e.g., external location = 'abfss://silver@adlsprod...'
            table path must be under 'abfss://silver@adlsprod/...'

CAUSE D: Wrong scope — role assigned on wrong container
  Check: Was the role granted on the storage account or a specific container?
  Fix: Grant role at the STORAGE ACCOUNT level, not just one container
```

### Problem 2 — Cluster Can't Start — "Timeout creating cluster resources"

```
Symptom: Cluster stuck in "Pending" for 15+ minutes then fails.

CAUSE A: NSG blocking control plane from reaching cluster VMs
  Fix: Ensure NSG has inbound rule:
       Source: Service Tag "AzureDatabricks"
       Destination: Your subnet
       Port: *
       Action: Allow

CAUSE B: No outbound internet access (NPIP clusters need NAT Gateway)
  Fix: Attach NAT Gateway to your Databricks subnets
       Ensure NAT Gateway has a public IP prefix

CAUSE C: Subnet too small
  Fix: /26 minimum (64 IPs) but recommend /24 for larger clusters
       One IP per VM + Azure reserved (5 IPs per subnet)

CAUSE D: Service delegation missing
  Fix: Both subnets must be delegated to Microsoft.Databricks/workspaces
```

### Problem 3 — "Serverless SQL Warehouse Can't Read Private ADLS"

```
Symptom: SQL Warehouse query fails — connection timeout to adlsprod.

CAUSE: Serverless compute can't reach ADLS via Private Endpoint without NCC.

Fix:
  1. Create a Private Endpoint in your VNet for the ADLS account
  2. Create an NCC in Databricks Account Console
  3. Add a Private Endpoint Rule to the NCC:
     Resource: ADLS account resource ID
     Sub-resource: dfs
  4. Approve the connection in Azure Portal:
     Storage Account → Networking → Private Endpoint Connections
  5. Attach NCC to your workspace
  6. Restart the SQL Warehouse
```

### Problem 4 — Unity Catalog: "Principal does not exist"

```
Symptom: GRANT SELECT ON TABLE x TO `user@company.com` fails.

CAUSE: User exists in Azure AD but hasn't been added to Databricks account.

Fix:
  1. Go to Databricks Account Console → User Management
  2. Add the user (sync from Azure AD SCIM if configured)
  3. Assign the user to the workspace
  4. Now run GRANT command — user exists in UC
```

### Problem 5 — Private DNS Resolution Not Working

```
Symptom: Cluster resolves adlsprod.dfs.core.windows.net to PUBLIC IP (20.x.x.x)
         instead of private IP (10.1.3.11). Connection fails due to firewall.

CAUSE: Private DNS Zone not linked to your VNet.

Fix:
  1. Check Private DNS Zones: privatelink.dfs.core.windows.net
  2. Check "Virtual network links":
     Is your Databricks VNet listed? If not → Add link
  3. Also check: if using custom DNS server (forwarder), ensure it forwards
     privatelink.* zones to Azure DNS (168.63.129.16)
     Otherwise custom DNS server can't resolve private endpoints.
```

---

## 12. Quick Reference Cheat Sheet

### Key Azure networking objects

| Object | What it is | Databricks use |
|---|---|---|
| VNet | Private network in Azure | Container for all resources |
| Subnet | Segment of VNet | Separate subnets for clusters |
| NSG | Firewall for subnets | Control cluster inbound/outbound |
| Private Endpoint | Private IP for Azure service | Private access to ADLS, KV, SQL |
| Private DNS Zone | DNS override for private IPs | Ensure clusters resolve PE IPs |
| NAT Gateway | Shared outbound IP for subnet | Cluster internet access (NPIP) |
| Azure Firewall | Inspect/filter all traffic | Enterprise traffic control |
| NCC | Links serverless to your PEs | Serverless private data access |
| Access Connector | Azure resource to hold MI | UC storage credential via MI |
| Managed Identity | Passwordless Azure identity | UC → ADLS auth |

### Key IP/port requirements for Databricks

```bash
# INBOUND to Databricks subnets (NSG)
Allow: AzureDatabricks service tag → Any    (control plane management)
Allow: VirtualNetwork → VirtualNetwork      (cluster ↔ cluster Spark traffic)

# OUTBOUND from Databricks subnets (NSG)
Allow: → AzureDatabricks  port 443          (control plane)
Allow: → AzureStorage     port 443          (DBFS, cluster init binaries)
Allow: → VirtualNetwork                     (cluster ↔ cluster)
Allow: → Internet  port 443                 (pip install, APIs) — via NAT GW

# ADLS Private Endpoint — DNS zones needed
privatelink.dfs.core.windows.net            (ADLS Gen2 DFS endpoint)
privatelink.blob.core.windows.net           (ADLS Gen2 Blob endpoint)

# Key Vault — DNS zone
privatelink.vaultcore.azure.net

# SQL Server — DNS zone
privatelink.database.windows.net
```

### Connection type decision tree

```
Do you need clusters to access private data sources?
  YES → Use VNet Injection
        └── Do you also need to deny public access to ADLS?
              YES → Create Private Endpoints + Private DNS Zones
              NO  → Service Endpoints (simpler but not fully private)

Do you use Serverless SQL Warehouses / Serverless Jobs?
  YES → Do you need private access to ADLS?
          YES → Set up NCC with Private Endpoint rules
          NO  → Add Databricks Stable Outbound IPs to ADLS firewall allowlist

Do you need the Databricks UI itself to be private?
  YES → Enable Front-End Private Link on workspace
        Create Private Endpoint for workspace
        Link Private DNS Zone: privatelink.azuredatabricks.net

Do clusters need internet access (pip install, external APIs)?
  YES → Attach NAT Gateway to Databricks subnets (if using No Public IP)
  NO  → Use No Public IP + restrict outbound NSG rules
```

### ADLS access permission matrix

```
Who needs access?        Azure RBAC role            Where granted
─────────────────────────────────────────────────────────────────
UC Storage Credential    Storage Blob Data          Storage account
(Managed Identity)       Contributor                or container

Classic cluster          Storage Blob Data          Storage account
(cluster-level MI)       Reader/Contributor         or container

Service Principal        Storage Blob Data          Storage account
(for external access)    Reader                     or container

User direct access       Storage Blob Data          Storage account
(NOT recommended)        Reader                     or container
← Use UC instead →
```
