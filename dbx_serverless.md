# Databricks Private Network Gateway (PNG) — Connectivity Explained with Examples

---

## Part 1 — The Old Approach: Private Endpoints Per Service (Before PNG)

Before PNG existed, Databricks serverless could only reach private resources through **Azure Private Link** — one Private Endpoint per resource. Here is what that looked like in practice.

### What is a Private Endpoint?

A Private Endpoint gives an Azure service (or your own service behind an Azure Load Balancer) a **private IP address inside your VNet**. Traffic to that service travels over the Microsoft backbone — never over the public internet.

```
┌─────────────────────────────────┐        ┌────────────────────────────────────────────────┐
│  Databricks Serverless Compute  │        │             Your Azure VNet                    │
│                                 │        │                                                │
│  Notebook / Job                 │──PE──▶ │  10.1.10.5  ← Private Endpoint for SQL DB     │
│                                 │──PE──▶ │  10.1.10.6  ← Private Endpoint for Storage    │
│                                 │──PE──▶ │  10.1.10.7  ← Private Endpoint for Key Vault  │
│                                 │──PE──▶ │  10.1.10.8  ← Private Endpoint for Snowflake  │
└─────────────────────────────────┘        └────────────────────────────────────────────────┘
                                           One endpoint created, approved & DNS-wired per resource
```

### How to Set Up a Private Endpoint for Azure SQL (Old Method)

**Step 1 — Create the Private Endpoint in Azure Portal**
- Go to your Azure SQL Server → Networking → Private endpoint connections → + Private endpoint
- Choose your VNet and a subnet (e.g., `pe-subnet`)
- Azure assigns it a private IP, e.g., `10.1.10.5`

**Step 2 — Approve the connection**
- The resource owner approves the connection request (or it auto-approves for resources in the same subscription)

**Step 3 — Wire up Private DNS**
- Create a Private DNS Zone `privatelink.database.windows.net`
- Add an A record: `myserver` → `10.1.10.5`
- Link the zone to the VNet so `myserver.database.windows.net` resolves to `10.1.10.5` inside the VNet

**Step 4 — Register the Private Endpoint in Databricks NCC**

Databricks also needs to know about this Private Endpoint so its serverless DNS can resolve the hostname correctly:

```http
POST https://accounts.azuredatabricks.net/api/2.0/accounts/{accountId}/network-connectivity-configs/{nccId}/private-endpoint-rules

{
  "resource_id": "/subscriptions/xxxxxxxx/resourceGroups/my-rg/providers/Microsoft.Sql/servers/myserver",
  "group_id": "sqlServer",
  "enabled": true
}
```

**Step 5 — Connect from a Databricks notebook**
```python
jdbc_url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=MyDB"

df = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", "dbo.orders") \
  .option("user", "myuser") \
  .option("password", dbutils.secrets.get("scope", "sql-pass")) \
  .load()
```

### Repeat for Every Single Resource

The same process above had to be done **individually** for each resource:

| Resource | Private DNS Zone | group_id in NCC |
|---|---|---|
| Azure SQL Database | `privatelink.database.windows.net` | `sqlServer` |
| Azure Data Lake Storage (ADLS) | `privatelink.dfs.core.windows.net` | `dfs` |
| Azure Blob Storage | `privatelink.blob.core.windows.net` | `blob` |
| Azure Key Vault | `privatelink.vaultcore.azure.net` | `vault` |
| Snowflake | `privatelink.snowflakecomputing.com` | `snowflakecomputing` |
| Azure Event Hub | `privatelink.servicebus.windows.net` | `namespace` |
| Azure Cosmos DB | `privatelink.documents.azure.com` | `Sql` |

Each one required: create PE → approve → DNS zone → A record → zone-VNet link → NCC registration. 10 resources = 10× this workflow.

### Real-World Example — NCC with Multiple Private Endpoint Rules

```http
# Register ADLS private endpoint
POST .../network-connectivity-configs/{nccId}/private-endpoint-rules
{
  "resource_id": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/mydatalake",
  "group_id": "dfs",
  "enabled": true
}

# Register Key Vault private endpoint
POST .../network-connectivity-configs/{nccId}/private-endpoint-rules
{
  "resource_id": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/my-kv",
  "group_id": "vault",
  "enabled": true
}

# Register Snowflake private endpoint (via Azure Load Balancer)
POST .../network-connectivity-configs/{nccId}/private-endpoint-rules
{
  "resource_id": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Network/privateLinkServices/snowflake-pls",
  "group_id": "snowflakecomputing",
  "enabled": true
}
```

Each call creates a pending connection that the resource owner must approve before traffic flows.

### Pain Points of the Per-Service Approach

- **Operational overhead:** 10 resources = 10 Private Endpoints, 10 DNS zones, 10 NCC registrations, 10 approvals
- **No VM / on-prem support:** Private Link only works with Azure PaaS services and services behind Azure Load Balancer. A SQL Server running on a plain VM? No Private Link. On-prem DB via ExpressRoute? No Private Link.
- **Scaling is painful:** Adding a new data source means a new endpoint, new DNS wiring, new NCC rule — often requiring coordination across Azure admin, network, and Databricks teams
- **No transitive routing:** A Private Endpoint connects you to exactly one resource — it does not give you access to anything else reachable from that resource's network

---

## Part 2 — The New Approach: Private Network Gateway (PNG)

## What Problem Does PNG Solve?

Databricks **serverless compute** runs in Databricks-managed infrastructure — not inside your Azure VNet. By default, it can only reach public endpoints or resources exposed via Private Link (one endpoint per resource). 

PNG punches a **tunnel** from Databricks serverless → into a subnet you delegate in your own Azure VNet. Once the tunnel is up, serverless jobs can reach **anything your VNet can reach** — on-prem databases, internal APIs, VMs — without setting up individual Private Link endpoints for each one.

```
┌─────────────────────────────────┐           ┌──────────────────────────────────────────┐
│  Databricks Serverless Compute  │           │         Your Azure VNet (e.g. eastus2)   │
│                                 │           │                                          │
│  Notebook / Job / DLT Pipeline  │──PNG Tunnel──▶  PNG Delegated Subnet (/28)          │
│                                 │           │        │                                 │
└─────────────────────────────────┘           │        ▼                                 │
                                              │   Internal SQL VM  ← reachable!         │
                                              │   On-Prem via ExpressRoute ← reachable! │
                                              │   Azure Firewall / egress ← reachable!  │
                                              └──────────────────────────────────────────┘
```

---

## Side-by-Side Comparison: Private Endpoints vs PNG

| | **Old: Private Endpoints per service** | **New: Private Network Gateway** |
|---|---|---|
| **Setup per new resource** | Create PE → approve → DNS zone → A record → NCC rule | Nothing — already routed if VNet can reach it |
| **Supports VMs** | No | Yes |
| **Supports on-prem via ExpressRoute/VPN** | No | Yes |
| **Transitive routing** | No | Yes |
| **Works for Azure PaaS (SQL DB, Storage, etc.)** | Yes (still recommended for PaaS) | Yes (lower priority than PE — PE wins if both exist) |
| **Admin effort at scale (10 resources)** | 10× workflows, 10 approvals | 1 PNG, add destination DNS names |
| **Traffic control** | Per-resource allow/deny | SPECIFIC_DESTINATIONS or ALL_TRAFFIC mode |
| **Can route through firewall** | No | Yes (ALL_TRAFFIC mode) |

> **Best practice:** Keep Private Endpoints for Azure PaaS services (Storage, SQL DB, Key Vault) — they remain highest priority and are the most secure path. Use PNG on top of that for everything Private Endpoints cannot cover: VMs, on-prem, custom APIs.

---

## Important: What PNG Does NOT Replace — Unity Catalog & ADLS Storage

> **Short answer: You still need Private Endpoints for every ADLS storage account backing Unity Catalog. PNG does not change this.**

This is one of the most common misunderstandings about PNG. Here is exactly why:

### Why storage is excluded from PNG

Azure Storage (ADLS Gen2 / Blob) is a **cloud-hosted Azure PaaS service**. Azure always routes traffic to it via **Azure Service Endpoints** — this is a platform-level enforcement that PNG cannot override. The Databricks documentation explicitly states:

> *"PNG connects to resources in your virtual networks. It does not connect to cloud-hosted services, such as storage (ADLS), which use service endpoints. Blob storage is always routed via SE and cannot be overridden by PNG."*

The traffic priority chain confirms this:
```
Request to mydatalake.dfs.core.windows.net  (Unity Catalog external location / managed storage)
  ──▶ Priority 1: Is there an NCC Private Endpoint rule for this storage account?
         YES → traffic goes via Private Link  ✓ (secure, private)
         NO  → Priority 2: Azure Service Endpoint → public endpoint of the storage account ✗
```

PNG sits at priority 3 — it never even gets evaluated for storage traffic.

### Current state: Private Endpoint required per storage account

Today, to keep Unity Catalog data access fully private from serverless compute, you need:

```
For each ADLS Gen2 storage account (Unity Catalog managed storage or external location):

  1. Create a Private Endpoint for the storage account (dfs endpoint)
  2. Register it in the NCC:

     POST .../network-connectivity-configs/{nccId}/private-endpoint-rules
     {
       "resource_id": "/subscriptions/xxx/.../storageAccounts/unitycatalog-storage-01",
       "group_id": "dfs",
       "enabled": true
     }

  3. Repeat for every storage account Unity Catalog touches
```

A typical Unity Catalog setup might have:
- 1 metastore-managed storage account
- 3–10 external location storage accounts (one per data domain / environment)
- Each one needs its own Private Endpoint and NCC rule

### After PNG: Storage still needs Private Endpoints

```
┌─────────────────────────────────────────────────────────────────────┐
│  What PNG helps with         │  What still needs Private Endpoints  │
├─────────────────────────────────────────────────────────────────────┤
│  SQL Server VM               │  ADLS Gen2 (Unity Catalog storage)   │
│  Oracle VM                   │  Azure Blob Storage                  │
│  On-prem DB (ExpressRoute)   │  Azure SQL Database (PaaS)           │
│  Internal REST APIs          │  Azure Key Vault                     │
│  Custom microservices        │  Snowflake                           │
│  Any VM-hosted service       │  Azure Event Hub                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Practical example — Unity Catalog with PNG

Suppose your workspace uses:
- `uc-managed-storage.dfs.core.windows.net` — Unity Catalog metastore managed storage
- `domain-raw.dfs.core.windows.net` — external location for raw data (VM-hosted ingestion writes here)
- `sqlvm.internal.contoso.com` — a SQL Server VM that serverless jobs query

Your NCC needs **both**:

```
# Still required — Private Endpoint for each storage account
POST .../private-endpoint-rules
{ "resource_id": ".../storageAccounts/uc-managed-storage", "group_id": "dfs", "enabled": true }

POST .../private-endpoint-rules
{ "resource_id": ".../storageAccounts/domain-raw", "group_id": "dfs", "enabled": true }

# New with PNG — replaces need for a PE on the SQL VM (which was impossible anyway)
POST .../private-network-gateways
{
  "gateway_name": "png-prod",
  "traffic_mode": "SPECIFIC_DESTINATIONS",
  "destinations": [
    { "destination_type": "DNS_NAME", "value": "sqlvm.internal.contoso.com" }
  ],
  ...
}
```

PNG adds connectivity for the VM. The storage Private Endpoints remain exactly as before.

### When will storage Private Endpoints go away?

Databricks has noted that PNG's current scope excludes cloud-hosted Azure PaaS storage. If/when Databricks extends PNG to cover storage, the documentation will state it explicitly. Until then, continue provisioning per-storage-account Private Endpoint rules for any Unity Catalog storage you want to keep off the public internet.

---

## Core Concepts

| Concept | What it is |
|---|---|
| **NCC** (Network Connectivity Config) | Account-level object that manages all serverless networking. PNG lives inside an NCC. |
| **PNG** (Private Network Gateway) | The actual tunnel object. Max 2 per NCC. |
| **Delegated Subnet** | A `/28` subnet in your VNet you hand over to Databricks. PNG injects into it. |
| **Traffic Mode** | `SPECIFIC_DESTINATIONS` (route named FQDNs only) or `ALL_TRAFFIC` (route everything through your VNet). |
| **private_dns_resolvers** | The DNS server PNG uses to resolve hostnames (e.g. your private DNS or Azure's `168.63.129.16`). |

---

## Example 1 — Accessing a SQL Server VM Inside Your VNet

**Scenario:** You have a SQL Server running on a VM at `sqlvm.internal.contoso.com` (IP `10.1.2.5`) inside your Azure VNet. Your Databricks serverless notebooks need to query it via JDBC.

### Step-by-step

**1. Delegate a subnet in your VNet (Azure Portal)**
- Go to your VNet → Subnets → create `png-subnet` with CIDR `10.1.100.0/28`
- Under "Subnet delegation" → select `Microsoft.Databricks/workspaces`
- Save

**2. Create the NCC (if not already existing)**
```http
POST https://accounts.azuredatabricks.net/api/2.0/accounts/{accountId}/network-connectivity-configs

{
  "name": "ncc-eastus2-prod",
  "region": "eastus2"
}
```
Response gives you: `"network_connectivity_config_id": "ad02b653-adb4-4e9f-a6f7-6c2836db7a72"`

**3. Create the PNG**
```http
POST https://accounts.azuredatabricks.net/api/2.0/accounts/{accountId}/network-connectivity-configs/ad02b653-adb4-4e9f-a6f7-6c2836db7a72/private-network-gateways

{
  "gateway_name": "png-sqlvm",
  "azure_cloud_connection": {
    "gateway_subnet": {
      "resource_id": "/subscriptions/xxxxxxxx/resourceGroups/my-rg/providers/Microsoft.Network/virtualNetworks/my-vnet/subnets/png-subnet"
    }
  },
  "private_dns_resolvers": [
    {
      "resolver_type": "IP_ADDRESS",
      "value": "10.1.0.4"
    }
  ],
  "traffic_mode": "SPECIFIC_DESTINATIONS",
  "destinations": [
    { "destination_type": "DNS_NAME", "value": "sqlvm.internal.contoso.com" }
  ]
}
```

**4. Wait for ESTABLISHED state (~2-5 min), then attach NCC to workspace**
```http
PATCH https://accounts.azuredatabricks.net/api/2.0/accounts/{accountId}/workspaces/{workspaceId}

{
  "network_connectivity_config_id": "ad02b653-adb4-4e9f-a6f7-6c2836db7a72"
}
```

**5. Connect from a Databricks notebook**
```python
jdbc_url = "jdbc:sqlserver://sqlvm.internal.contoso.com:1433;databaseName=MyDB"

df = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", "dbo.sales") \
  .option("user", "myuser") \
  .option("password", dbutils.secrets.get("my-scope", "sql-password")) \
  .load()

df.show()
```

Traffic flow: Notebook → PNG tunnel → `png-subnet` → SQL VM. No public IP, no Private Link endpoint needed.

---

## Example 1b — Under the Hood: How PNG Injects into Your Subnet and Uses Its IPs

This example walks through exactly what Azure does to your subnet when PNG is created, and how traffic from Databricks serverless appears to originate from an IP inside **your own address space**.

### Your subnet before PNG

You created `png-subnet` with CIDR `10.1.100.0/28`. A `/28` gives you 16 addresses:

```
10.1.100.0   — Network address (reserved by Azure)
10.1.100.1   — Default gateway (reserved by Azure)
10.1.100.2   — Azure DNS (reserved by Azure)
10.1.100.3   — Reserved by Azure
10.1.100.4   ─┐
10.1.100.5    │
10.1.100.6    │  Available — PNG will consume these
10.1.100.7    │
  ...         │
10.1.100.14  ─┘
10.1.100.15  — Broadcast (reserved by Azure)
```

Nothing is deployed here yet. The subnet is empty and delegated to `Microsoft.Databricks/workspaces`.

### What happens when you POST the PNG creation API

When you call:
```http
POST .../network-connectivity-configs/{nccId}/private-network-gateways
{
  "gateway_name": "my-png",
  "azure_cloud_connection": {
    "gateway_subnet": {
      "resource_id": "/subscriptions/xxx/resourceGroups/my-rg/providers/Microsoft.Network/virtualNetworks/my-vnet/subnets/png-subnet"
    }
  },
  ...
}
```

Databricks uses the Azure subnet delegation to **inject gateway nodes (NICs) directly into your subnet**. Concretely:

1. Azure allocates private IPs from `png-subnet` to Databricks-managed NICs — e.g., `10.1.100.4`, `10.1.100.5`
2. These NICs are owned by Databricks infrastructure but live inside **your VNet address space**
3. A tunnel is established between Databricks serverless compute and these NICs
4. From the perspective of the rest of your VNet, traffic arriving from `10.1.100.4` looks like it came from a regular VM inside your network

```
Before PNG creation:
  png-subnet (10.1.100.0/28) — empty, delegated

After PNG creation (state: ESTABLISHED):
  png-subnet (10.1.100.0/28)
    ├── 10.1.100.4  ← PNG gateway NIC (Databricks-managed, injected by Azure delegation)
    ├── 10.1.100.5  ← PNG gateway NIC (second node for HA)
    └── remaining IPs still free
```

You can confirm this in the Azure Portal:
- Navigate to your VNet → Subnets → `png-subnet`
- Under **Connected devices**, you will see network interfaces attached by Databricks

### What the SQL VM sees

Your SQL Server VM at `10.1.2.5` has a firewall rule allowing connections from `10.1.100.0/28`:

```sql
-- On the SQL Server VM, allow connections from the PNG subnet
-- (Windows Firewall / SQL Server firewall)
-- Source: 10.1.100.0/28
-- Port: 1433
```

When a Databricks serverless notebook runs a JDBC query:

```
Databricks Serverless Worker
        │
        │  (encrypted tunnel)
        ▼
PNG Gateway NIC — IP: 10.1.100.4  (inside your png-subnet)
        │
        │  standard TCP/IP inside your VNet
        ▼
SQL Server VM — IP: 10.1.2.5, Port: 1433
```

The SQL VM's connection log shows the client IP as `10.1.100.4` — an address from **your own subnet**, not a Databricks public IP. This is why you firewall the PNG subnet CIDR, not Databricks' public ranges.

### NSG rules you need on the PNG subnet

Azure Network Security Groups apply to the PNG subnet just like any other subnet. You must allow outbound traffic on the ports your resources use:

```
NSG on png-subnet (or destination subnet):

Outbound rules (from png-subnet):
┌──────────────────┬───────────┬──────────┬────────────────────────┬────────┐
│ Name             │ Source    │ Dest     │ Dest Port              │ Action │
├──────────────────┼───────────┼──────────┼────────────────────────┼────────┤
│ Allow-SQL        │ 10.1.100.0/28 │ 10.1.2.5 │ 1433              │ Allow  │
│ Allow-Oracle     │ 10.1.100.0/28 │ 10.1.3.10│ 1521              │ Allow  │
│ Allow-DNS        │ 10.1.100.0/28 │ 10.1.0.4 │ 53                │ Allow  │
│ DenyAll          │ *         │ *        │ *                      │ Deny   │
└──────────────────┴───────────┴──────────┴────────────────────────┴────────┘

Inbound rules (to destination VM subnet):
┌──────────────────┬───────────────┬──────────┬──────┬────────┐
│ Name             │ Source        │ Dest     │ Port │ Action │
├──────────────────┼───────────────┼──────────┼──────┼────────┤
│ Allow-From-PNG   │ 10.1.100.0/28 │ *        │ 1433 │ Allow  │
└──────────────────┴───────────────┴──────────┴──────┴────────┘
```

### IP address planning tip — why /28 is enough even for 30+ concurrent jobs

A common misconception: "30 serverless jobs running = 30 IPs needed in the PNG subnet." This is **not how it works**.

The PNG subnet IPs are used **only for the gateway injection nodes** — the NICs that Databricks injects to establish the tunnel. Databricks injects exactly **2 NICs** regardless of how many jobs are running:

```
png-subnet (10.1.100.0/28)
  ├── 10.1.100.4  ← PNG gateway NIC #1 (active)
  ├── 10.1.100.5  ← PNG gateway NIC #2 (standby for HA)
  └── 10.1.100.6 … 10.1.100.14  ← unused, not allocated per job
```

All 30 (or 300) concurrent serverless workers **share the same 2 gateway NICs**. The NICs act like a NAT gateway — many upstream connections multiplex through a small number of IPs using port tracking (SNAT). The workers themselves live in Databricks-managed compute, completely outside your subnet.

```
Serverless Worker 1  ─┐
Serverless Worker 2   │
Serverless Worker 3   ├──[encrypted tunnel]──▶ PNG NIC 10.1.100.4 ──▶ SQL VM 10.1.2.5
...                   │
Serverless Worker 30 ─┘

All 30 workers share the same 2 gateway IPs. The SQL VM sees source IP 10.1.100.4 for all of them.
```

**What would actually require more IPs?**

| Situation | IPs needed in png-subnet |
|---|---|
| 1 PNG (any number of jobs) | 2 (always) |
| 2 PNGs in the same NCC (max allowed) | 4 (2 per PNG, each in its own subnet) |
| 100 concurrent serverless workers | Still 2 — worker count is irrelevant |

**What you *do* need to scale for high concurrency is on the destination side** — e.g., SQL Server max connections, NSG connection tracking limits, or the DNS resolver's capacity — not the PNG subnet.

A `/28` gives you 16 addresses (11 usable after Azure reserves 5). With 2 used by PNG, you have 9 spare — more than enough. The minimum is `/28` per Databricks documentation.

```
Recommended subnet layout:
  10.1.100.0/28  — png-subnet-1   (for PNG #1, delegated to Databricks)
  10.1.101.0/28  — png-subnet-2   (for PNG #2 if needed, each PNG needs its own subnet)
  10.1.10.0/24   — pe-subnet      (for Private Endpoints — separate, not delegated)
```

---

## Example 2 — On-Premises Database via ExpressRoute

**Scenario:** Your company has an on-premises Oracle DB at `oracle-prod.corp.contoso.com`, reachable from your Azure VNet via an ExpressRoute circuit. Databricks serverless pipelines need to read from it.

Since ExpressRoute connects to your VNet, once PNG tunnels into your VNet, the **transitive route** through ExpressRoute is automatically available.

**PNG creation (key differences from Example 1):**
```json
{
  "gateway_name": "png-onprem",
  "azure_cloud_connection": {
    "gateway_subnet": {
      "resource_id": "/subscriptions/xxxxxxxx/resourceGroups/my-rg/providers/Microsoft.Network/virtualNetworks/my-vnet/subnets/png-subnet"
    }
  },
  "private_dns_resolvers": [
    {
      "resolver_type": "IP_ADDRESS",
      "value": "10.1.0.4"
    }
  ],
  "traffic_mode": "SPECIFIC_DESTINATIONS",
  "destinations": [
    { "destination_type": "DNS_NAME", "value": "oracle-prod.corp.contoso.com" },
    { "destination_type": "DNS_NAME", "value": "corp.contoso.com" }
  ]
}
```
> Note: `corp.contoso.com` as a destination also matches `oracle-prod.corp.contoso.com` and any other `*.corp.contoso.com` — suffix matching is supported.

**In a Databricks DLT pipeline:**
```python
import cx_Oracle

@dlt.table
def raw_oracle_sales():
    conn = cx_Oracle.connect(
        user="dbuser",
        password=dbutils.secrets.get("scope", "oracle-pass"),
        dsn="oracle-prod.corp.contoso.com:1521/ORCL"
    )
    return spark.read.jdbc(
        url="jdbc:oracle:thin:@oracle-prod.corp.contoso.com:1521/ORCL",
        table="SALES.ORDERS"
    )
```

---

## Example 3 — All Traffic Through Azure Firewall (ALL_TRAFFIC mode)

**Scenario:** Your security team requires all outbound traffic from Databricks serverless to pass through an Azure Firewall at `10.1.50.4` for content inspection and logging.

```json
{
  "gateway_name": "png-firewall-egress",
  "azure_cloud_connection": {
    "gateway_subnet": {
      "resource_id": "/subscriptions/xxxxxxxx/resourceGroups/my-rg/providers/Microsoft.Network/virtualNetworks/my-vnet/subnets/png-subnet"
    }
  },
  "private_dns_resolvers": [
    {
      "resolver_type": "IP_ADDRESS",
      "value": "168.63.129.16"
    }
  ],
  "traffic_mode": "ALL_TRAFFIC",
  "destinations": [
    { "destination_type": "DNS_NAME", "value": "ALL_TRAFFIC" }
  ]
}
```

With this config:
- ALL outbound traffic from serverless → PNG tunnel → your VNet → Azure Firewall → internet
- Azure Firewall logs, inspects, and enforces policies on every connection
- Exception: Private Link and Azure Service Endpoints still use their native paths (PNG does not override them)

**Traffic routing priority in this scenario:**
```
Request to storage.blob.core.windows.net
  → Matched by Azure Service Endpoint (priority 2) → NOT through PNG

Request to api.salesforce.com
  → No Private Link or SE match → Goes through PNG (ALL_TRAFFIC) → Firewall → internet

Request to sqlvm.internal.contoso.com
  → No Private Link match → Goes through PNG (ALL_TRAFFIC) → internal VM
```

---

## Example 4 — Salesforce with Dedicated Egress IP (BYO-NAT)

**Scenario:** Salesforce requires connections to come from a fixed, allowlisted IP address. You want Databricks serverless to egress through your own NAT Gateway (with a static public IP).

Architecture:
```
Databricks Serverless → PNG Tunnel → png-subnet → NAT Gateway (static IP: 40.1.2.3) → Salesforce
```

You configure the NAT Gateway on your VNet's `png-subnet` (or route traffic through it), and Salesforce whitelists `40.1.2.3`.

**PNG config:**
```json
{
  "gateway_name": "png-salesforce",
  "traffic_mode": "SPECIFIC_DESTINATIONS",
  "destinations": [
    { "destination_type": "DNS_NAME", "value": "mycompany.my.salesforce.com" }
  ],
  "private_dns_resolvers": [
    { "resolver_type": "IP_ADDRESS", "value": "168.63.129.16" }
  ]
}
```

**Databricks notebook:**
```python
import simple_salesforce

sf = simple_salesforce.Salesforce(
    username=dbutils.secrets.get("scope", "sf-user"),
    password=dbutils.secrets.get("scope", "sf-pass"),
    security_token=dbutils.secrets.get("scope", "sf-token"),
    instance_url="https://mycompany.my.salesforce.com"
)

records = sf.query("SELECT Id, Name, Amount FROM Opportunity WHERE StageName = 'Closed Won'")
df = spark.createDataFrame(records["records"])
df.write.format("delta").mode("overwrite").saveAsTable("salesforce.opportunities")
```

---

## Traffic Priority — Quick Reference

When a Databricks serverless job makes an outbound connection, routing is decided in this order:

```
1. NCC Private Endpoint Rules  ──▶  Direct private link to Azure PaaS (Storage, SQL DB, Snowflake, etc.)
        ↓ no match
2. Azure Service Endpoints     ──▶  Microsoft backbone to public-facing Azure PaaS (*.database.windows.net, etc.)
        ↓ no match
3. Private Network Gateway     ──▶  Through your VNet (your rules: SPECIFIC_DESTINATIONS or ALL_TRAFFIC)
        ↓ no match
4. Databricks default egress   ──▶  Shared Databricks NAT / internet egress
```

> **Important:** Blob Storage (`*.blob.core.windows.net`) is always routed via Service Endpoint — PNG cannot override this by design.

---

## DNS — How Name Resolution Works

PNG forwards DNS queries for configured destinations to the resolver you specify.

| Scenario | Resolver to use |
|---|---|
| Resources in Private DNS Zone | Your private DNS server IP (e.g. `10.1.0.4`) — and ensure the Private DNS Zone is linked to the PNG subnet's VNet |
| Public FQDNs (internet resources) | Azure default: `168.63.129.16` |
| On-premises DNS (via ExpressRoute) | Your on-prem forwarder IP reachable from the VNet |

**Example:** If `sqlvm.internal.contoso.com` is registered in an Azure Private DNS Zone `internal.contoso.com`, you must:
1. Link `internal.contoso.com` zone to the VNet containing `png-subnet`
2. Set `private_dns_resolvers` to `168.63.129.16` (Azure DNS knows about the linked zone)

---

## Common Troubleshooting Scenarios

### Gateway stuck in CREATING
```
Cause:    Subnet not delegated, or subnet too small
Fix:      Verify delegation to Microsoft.Databricks/workspaces; subnet must be /28 or larger
```

### DNS resolves but connection times out
```
Cause:    NSG blocking traffic on png-subnet or destination subnet
Fix:      Allow inbound on the destination port (e.g., 1433 for SQL) from png-subnet CIDR in NSG rules
```

### JDBC fails even though DNS resolves
```
Cause:    Database firewall doesn't allow connections from PNG subnet's IP range
Fix:      Add png-subnet CIDR (e.g., 10.1.100.0/28) to the database's firewall allow-list
```

### Traffic to Azure SQL still not going through PNG
```
Cause:    *.database.windows.net matches Azure Service Endpoint (priority 2 wins over PNG)
Fix:      If you need PNG for this, check SE configuration; for private-only SQL, ensure no SE is enabled
```

---

## Summary

PNG is essentially a **managed VPN tunnel** from Databricks serverless into your Azure VNet. You configure it once at the NCC level, attach it to workspaces, and every serverless product (notebooks, jobs, DLT pipelines) in those workspaces automatically gets access to whatever your VNet can reach — without any per-resource Private Link setup.
