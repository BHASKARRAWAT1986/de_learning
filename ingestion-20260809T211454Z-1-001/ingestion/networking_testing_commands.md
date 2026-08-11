# Networking Testing Commands — Azure & Databricks

> Quick reference for verifying connectivity, DNS resolution, ports, and routing.
> Commands are grouped by what you're testing. Run them from:
> - **Azure VM** (SSH into it via terminal)
> - **Databricks Notebook** (`%sh` magic command for shell)
> - **Your local machine** (Windows PowerShell or WSL)
> - **Azure Cloud Shell** (browser-based bash at shell.azure.com)

---

## Table of Contents

1. [DNS Resolution — nslookup / dig / host](#1-dns-resolution)
2. [TCP Port Reachability — nc / ncat / telnet](#2-tcp-port-reachability)
3. [Ping — ICMP Reachability](#3-ping)
4. [Traceroute — Path Tracing](#4-traceroute)
5. [HTTP/HTTPS — curl / wget](#5-httphttps-connectivity)
6. [Route Table — Check Local Routing](#6-route-table)
7. [Network Interface Info — ip / ifconfig](#7-network-interface-info)
8. [Active Connections — ss / netstat](#8-active-connections)
9. [Firewall / NSG Verification from Azure CLI](#9-azure-cli-nsg--connectivity-checks)
10. [Databricks-Specific Tests (from Notebooks)](#10-databricks-specific-tests)
11. [Storage Account Connectivity Tests](#11-storage-account-connectivity-tests)
12. [On-Prem / VPN Connectivity Tests](#12-on-prem--vpn-connectivity)
13. [Quick Cheat Sheet](#13-quick-cheat-sheet)

---

## 1. DNS Resolution

### `nslookup` — Ask a DNS server what IP a hostname resolves to

```bash
# Basic: resolve a hostname using the default DNS server
nslookup <hostname>

# Example: resolve ADLS Gen2 storage account
nslookup mystorageaccount.dfs.core.windows.net

# Expected OUTSIDE VNet (public):
#   Address: 20.150.x.x   (public Azure IP)
#
# Expected INSIDE VNet with Private Endpoint + Private DNS Zone:
#   Address: 10.0.3.4   (private IP in YOUR subnet)
#   ✅ Private Endpoint is working correctly

# Query a SPECIFIC DNS server instead of the default
nslookup <hostname> <dns_server_ip>
nslookup mystorageaccount.dfs.core.windows.net 8.8.8.8         # Use Google DNS
nslookup mystorageaccount.dfs.core.windows.net 168.63.129.16   # Use Azure DNS
# Why: compare results from different DNS servers to debug resolution override

# Check if Private DNS Zone is overriding public DNS
nslookup mystorageaccount.dfs.core.windows.net 168.63.129.16
# If returns 10.x.x.x → Private DNS Zone is active
# If returns 20.x.x.x → Private DNS Zone NOT linked or missing A record
```

---

### `dig` — More detailed DNS lookup (Linux/WSL/Mac)

```bash
# Install if missing
sudo apt-get install -y dnsutils

# Basic resolve
dig mystorageaccount.dfs.core.windows.net

# Short output (IP only)
dig +short mystorageaccount.dfs.core.windows.net

# Show full answer section
dig +noall +answer mystorageaccount.dfs.core.windows.net

# Query a specific DNS server
dig @168.63.129.16 mystorageaccount.dfs.core.windows.net
dig @8.8.8.8 mystorageaccount.blob.core.windows.net

# Trace the full DNS resolution chain (who delegates to whom)
dig +trace mystorageaccount.dfs.core.windows.net
# Useful for debugging: shows ROOT → TLD → Azure DNS → Private DNS Zone chain

# Check what CNAME chain is set up (Private Endpoint uses a CNAME to privatelink)
dig mystorageaccount.blob.core.windows.net CNAME
# Expected: mystorageaccount.blob.core.windows.net → mystorageaccount.privatelink.blob.core.windows.net
#           mystorageaccount.privatelink.blob.core.windows.net → 10.0.3.4
```

---

### `host` — Simple DNS lookup

```bash
host mystorageaccount.dfs.core.windows.net
# Returns: mystorageaccount.dfs.core.windows.net has address 10.0.3.4

host -t CNAME mystorageaccount.blob.core.windows.net
# Shows CNAME records
```

---

### What DNS results tell you

```
Scenario                         | nslookup returns  | Meaning
---------------------------------|-------------------|----------------------------------
Public internet access           | 20.x.x.x          | Going through public Azure IP
Private Endpoint working         | 10.x.x.x          | Private DNS Zone override active
Wrong DNS (no PE awareness)      | 20.x.x.x          | VM using external DNS, not Azure DNS
PE exists but DNS not linked     | 20.x.x.x          | Private DNS Zone not linked to VNet
PE exists, DNS works, but 403    | 10.x.x.x          | Auth problem (token/RBAC), not DNS
```

---

## 2. TCP Port Reachability

### `nc` / `ncat` — Test if a specific TCP port is open and reachable

```bash
# Install if missing
sudo apt-get install -y netcat-openbsd   # or: ncat

# Test if a port is open (returns immediately)
nc -zv <hostname_or_ip> <port>

# -z = scan mode (don't send data, just check if port is open)
# -v = verbose (show success/failure message)

# Examples:

# Test ADLS private endpoint (HTTPS port 443)
nc -zv mystorageaccount.dfs.core.windows.net 443
# Success: Connection to mystorageaccount.dfs.core.windows.net 443 port [tcp/https] succeeded!
# Failure: nc: connect to ... port 443 (tcp) failed: Connection refused
#          nc: connect to ... port 443 (tcp) timed out   ← blocked by NSG/firewall

# Test on-prem PostgreSQL
nc -zv 192.168.0.4 5432
# If timeout: NSG blocking port 5432, or route missing, or firewall on VM

# Test on-prem Oracle
nc -zv 192.168.0.5 1521

# Test SQL Server
nc -zv 10.0.2.5 1433

# Test Key Vault private endpoint
nc -zv myvault.vault.azure.net 443

# Test with timeout (don't wait forever)
nc -zv -w 5 192.168.0.4 5432
# -w 5 = timeout after 5 seconds (default is forever)

# Test UDP port
nc -zvu <ip> <port>

# Multiple ports at once (scan a range)
nc -zv 10.0.2.5 22 80 443 1433
```

---

### `telnet` — Old-school TCP port test (available everywhere)

```bash
# Test if port is open
telnet <hostname> <port>
telnet mystorageaccount.dfs.core.windows.net 443

# If port is OPEN: screen goes blank (connected to the service)
# If port is CLOSED/BLOCKED: "Connection refused" or hangs

# Exit telnet: Ctrl+] then type "quit"

# Windows PowerShell equivalent:
Test-NetConnection -ComputerName mystorageaccount.dfs.core.windows.net -Port 443
# Expected if open:
#   TcpTestSucceeded : True
# Expected if blocked:
#   TcpTestSucceeded : False
```

---

### Windows PowerShell — `Test-NetConnection`

```powershell
# Test TCP port
Test-NetConnection -ComputerName "mystorageaccount.dfs.core.windows.net" -Port 443

# Test with traceroute included
Test-NetConnection -ComputerName "10.0.2.5" -Port 1433 -TraceRoute

# Simple ping test
Test-NetConnection -ComputerName "10.0.1.4"

# Output columns:
# ComputerName         : mystorageaccount.dfs.core.windows.net
# RemoteAddress        : 10.0.3.4          ← confirms Private Endpoint
# RemotePort           : 443
# InterfaceAlias       : Ethernet           ← which NIC
# TcpTestSucceeded     : True               ← port is reachable
```

---

## 3. Ping

### `ping` — ICMP echo (layer 3, not TCP)

```bash
# Basic ping
ping <ip_or_hostname>
ping 10.0.1.4

# Ping with count (stop after N packets)
ping -c 4 10.0.1.4       # Linux
ping -n 4 10.0.1.4       # Windows

# Ping with specific packet size (test MTU)
ping -s 1400 10.0.2.5    # Send 1400-byte packets
# Why: if large packets fail but small succeed → MTU mismatch (common over VPN)

# Continuous ping (Linux)
ping 10.0.2.5             # Ctrl+C to stop

# Windows ping is 4 packets by default; use -t for continuous
ping -t 10.0.2.5          # Ctrl+C to stop
```

#### IMPORTANT — When ping fails but connection works

```
NSGs and firewalls block ICMP (ping) by default.
Azure VMs often DON'T respond to ping even when TCP ports work fine.

Rule of thumb:
  ping fails         → could be NSG, firewall, or routing issue
  nc -zv port 443 succeeds → VM is reachable and port is open
  → ping failure is a RED HERRING in Azure

To confirm VM is alive when ping fails:
  Use nc/telnet to test a specific TCP port instead.
  Or use Azure Portal → VM → "Run command" → RunShellScript
```

---

## 4. Traceroute

### `traceroute` — Show each hop between you and the destination

```bash
# Install if missing
sudo apt-get install -y traceroute

# Basic traceroute
traceroute <hostname_or_ip>
traceroute mystorageaccount.dfs.core.windows.net
traceroute 192.168.0.4

# Use TCP instead of UDP (better through firewalls)
traceroute -T -p 443 mystorageaccount.dfs.core.windows.net
# -T = use TCP
# -p 443 = target port 443 (HTTPS)

# Use ICMP (like Windows tracert)
traceroute -I 10.0.2.5

# Set max hops (default 30)
traceroute -m 15 10.0.2.5

# Don't resolve hostnames (faster, shows raw IPs)
traceroute -n 10.0.2.5
```

---

### What traceroute output tells you

```
$ traceroute -n mystorageaccount.dfs.core.windows.net

traceroute to mystorageaccount.dfs.core.windows.net (10.0.3.4), 30 hops max
 1   10.0.2.1    0.5 ms   0.4 ms   0.4 ms    ← Default gateway of your subnet
 2   10.0.3.4    1.2 ms   1.1 ms   1.0 ms    ← Destination (Private Endpoint NIC)

GOOD: Only 1-2 hops, destination is a 10.x.x.x (private IP) ✅
      → Private Endpoint is routing traffic within the VNet

----------------------------------------

$ traceroute -n mystorageaccount.dfs.core.windows.net

 1   10.0.2.1    0.5 ms
 2   * * *                                   ← Azure backbone (doesn't respond to ICMP)
 3   * * *
 4   20.150.x.x  2.5 ms                     ← Public Azure IP reached!

BAD: Traffic went to public IP → Private DNS Zone not working,
     or NSG is blocking the private path, or PE was deleted. ❌
```

---

### `tracert` — Windows equivalent

```powershell
# Windows (cmd or PowerShell)
tracert mystorageaccount.dfs.core.windows.net
tracert -d 10.0.2.5    # -d = don't resolve hostnames
```

---

### `mtr` — Combines ping + traceroute in real time (Linux)

```bash
# Install
sudo apt-get install -y mtr

# Run
mtr mystorageaccount.dfs.core.windows.net
mtr -n 10.0.2.5     # No hostname resolution

# Shows live packet loss per hop — great for spotting intermittent issues
# Press q to quit
```

---

## 5. HTTP/HTTPS Connectivity

### `curl` — Test HTTP endpoints, check SSL certs, download

```bash
# Basic HTTPS GET (check if endpoint responds)
curl https://mystorageaccount.dfs.core.windows.net

# Check HTTP status code only
curl -o /dev/null -s -w "%{http_code}\n" https://mystorageaccount.blob.core.windows.net
# 200 = OK, 403 = Auth required (but reachable!), 000 = connection failed

# Verbose mode — shows SSL handshake, headers, TLS cert info
curl -v https://mystorageaccount.dfs.core.windows.net
# Look for:
#   * Connected to 10.0.3.4 port 443   ← private IP = PE is working
#   * SSL certificate verify OK         ← cert is valid
#   < HTTP/1.1 403                      ← reachable (403 = auth issue, not connectivity)

# Check what IP it connects to (confirm private vs public)
curl -v https://mystorageaccount.dfs.core.windows.net 2>&1 | grep "Connected to"
# Expected with PE: Connected to mystorageaccount.dfs.core.windows.net (10.0.3.4) port 443

# Send a HEAD request (less data, still confirms connectivity)
curl -I https://mystorageaccount.blob.core.windows.net

# Test with a timeout
curl --connect-timeout 5 https://mystorageaccount.dfs.core.windows.net

# Check your own outbound (public) IP (useful for NAT Gateway testing)
curl https://ifconfig.me
curl https://api.ipify.org
# If NAT Gateway is set up: returns NAT Gateway public IP
# If VM has its own public IP: returns that IP

# Test Azure metadata endpoint (inside Azure VMs only)
curl http://169.254.169.254/metadata/instance?api-version=2021-02-01 -H "Metadata:true"
# Returns JSON with VM name, region, subscription ID, etc.
# Useful for confirming which VM/region you're in

# Download Azure instance metadata to check network config
curl -s http://169.254.169.254/metadata/instance/network?api-version=2021-02-01 \
  -H "Metadata:true" | python3 -m json.tool
```

---

### `wget` — Alternative to curl

```bash
# Download a file (or test connectivity)
wget -q -O /dev/null https://mystorageaccount.blob.core.windows.net
# -q = quiet, -O /dev/null = discard output

# Check spider mode (don't download, just check if URL is reachable)
wget --spider https://mystorageaccount.dfs.core.windows.net
```

---

### Azure Storage-specific curl tests

```bash
# List blobs in a container (anonymous or with SAS token)
curl "https://mystorageaccount.blob.core.windows.net/bronze?restype=container&comp=list"
# Expected with public access disabled: 403 or blocked

# With a SAS token
SAS_TOKEN="?sv=2021-06-08&ss=b&srt=co&sp=rwdlacupiytfx&..."
curl "https://mystorageaccount.blob.core.windows.net/bronze${SAS_TOKEN}&restype=container&comp=list"
```

---

## 6. Route Table

### `ip route` — Show the routing table (Linux)

```bash
# Show all routes
ip route
# or
ip route show

# Example output from a VNet-injected Databricks cluster VM:
# default via 10.5.2.1 dev eth0 proto dhcp
# 10.5.0.0/16 dev eth0 proto kernel scope link src 10.5.2.4
# 168.63.129.16 via 10.5.2.1 dev eth0   ← Azure DNS
# 169.254.169.254 via 10.5.2.1 dev eth0  ← Azure Instance Metadata Service

# Show the route for a SPECIFIC destination
ip route get 192.168.0.4
# Output: 192.168.0.4 via 10.5.2.1 dev eth0
# Tells you: packets to 192.168.0.4 go via the default gateway 10.5.2.1

# If you have a UDR (User Defined Route) forcing traffic to a firewall:
ip route get 192.168.0.4
# Output: 192.168.0.4 via 10.0.10.4 dev eth0
#                         ^^^^^^^^^^
#                         Azure Firewall private IP (hub VNet)
# ✅ This confirms UDR is forcing traffic through the firewall
```

---

### `route` — Legacy route command

```bash
route -n         # Show routing table (numeric IPs, no hostname resolution)
route print      # Windows equivalent
```

---

### Windows — `netsh` and `route`

```powershell
# Show routing table
route print

# Show routes to a specific network
route print 10.0.0.0

# PowerShell: get routes
Get-NetRoute | Where-Object { $_.DestinationPrefix -like "10.*" }
```

---

## 7. Network Interface Info

### `ip addr` — Show all network interfaces and IPs

```bash
# Show all interfaces
ip addr
# or short form:
ip a

# Example output from a Databricks cluster VM:
# 2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP>
#     inet 10.5.2.4/26 brd 10.5.2.63 scope global eth0
#                ↑ private IP from dbr-private-subnet (10.5.2.0/26)

# Show only a specific interface
ip addr show eth0

# Confirm which subnet the VM is in (check the CIDR suffix)
ip addr | grep "inet "
# inet 10.5.2.4/26   → /26 = 64 IPs → confirms it's in dbr-private-subnet

# From Databricks notebook:
%sh ip addr | grep "inet "
```

---

### `ifconfig` — Old-school interface info

```bash
ifconfig          # Show all interfaces
ifconfig eth0     # Show specific interface
```

---

### Windows — `ipconfig`

```powershell
ipconfig                 # Show all IP configs
ipconfig /all            # Full details including DNS servers, DHCP server
ipconfig /flushdns       # Flush DNS cache (useful when testing DNS changes)
ipconfig /displaydns     # Show cached DNS entries
```

---

## 8. Active Connections

### `ss` — Show active sockets/connections (modern replacement for netstat)

```bash
# Show all TCP connections
ss -tn

# Show listening ports
ss -tlnp
# -t = TCP, -l = listening, -n = numeric, -p = show process

# Show all connections including Unix sockets
ss -a

# Show established TCP connections
ss -tn state established

# Filter by port
ss -tn sport = :443     # Connections using local port 443
ss -tn dport = :5432    # Connections to destination port 5432 (PostgreSQL)

# Show socket stats summary
ss -s

# Example output:
# Netid State  Recv-Q Send-Q   Local Address:Port   Peer Address:Port
# tcp   ESTAB  0      0        10.5.2.4:52834       10.5.2.6:443      ← to PE
# tcp   ESTAB  0      0        10.5.2.4:44510       20.150.x.x:443    ← to internet!
# The second line means some traffic is still going public — check your DNS
```

---

### `netstat` — Legacy connections viewer

```bash
# Show all active TCP connections
netstat -tn

# Show listening ports
netstat -tlnp

# Show all connections with process names
netstat -tulnp

# Windows:
netstat -ano          # All connections with PID
netstat -b            # Show process name (requires admin)
```

---

## 9. Azure CLI NSG & Connectivity Checks

### Check effective NSG rules on a VM's NIC

```bash
# Find the NIC name for a VM
NIC_NAME=$(az vm show \
  --resource-group rg-network-lab \
  --name vm-web \
  --query "networkProfile.networkInterfaces[0].id" \
  -o tsv | sed 's|.*/||')

# Show EFFECTIVE security rules (merges subnet NSG + NIC NSG)
az network nic show-effective-nsg \
  --resource-group rg-network-lab \
  --name $NIC_NAME \
  --output table

# Why: helps debug "why can't this VM reach port X" — shows the actual merged rules
# Azure evaluates rules from LOWEST to HIGHEST priority; first match wins.
```

---

### Check effective routing on a VM's NIC

```bash
# Show EFFECTIVE routes (merges system routes + UDR routes)
az network nic show-effective-route-table \
  --resource-group rg-network-lab \
  --name $NIC_NAME \
  --output table

# Look for:
#   NextHopType: VirtualNetworkGateway  → traffic going to VPN/ER
#   NextHopType: VirtualAppliance       → traffic going through firewall (UDR)
#   NextHopType: Internet               → traffic going to internet
#   NextHopType: None                   → traffic is DROPPED (blackhole)
```

---

### Use Azure Network Watcher — IP Flow Verify

```bash
# Ask Azure: "Would an NSG block traffic from IP X to IP Y on port Z?"
az network watcher test-ip-flow \
  --resource-group rg-network-lab \
  --vm vm-web \
  --direction Inbound \
  --protocol TCP \
  --local 10.0.1.4 \
  --local-port 80 \
  --remote 20.5.6.7 \
  --remote-port "*"

# Output:
#   "access": "Allow",
#   "ruleName": "Allow-HTTP-Inbound"
# ✅ Or:
#   "access": "Deny",
#   "ruleName": "DenyAllInBound"
# ❌ This tells you EXACTLY which rule is blocking/allowing

# Portal equivalent:
# Network Watcher → IP flow verify → fill in source/dest IPs and port
```

---

### Use Azure Network Watcher — Connection Troubleshoot

```bash
# End-to-end connectivity check between two Azure resources
az network watcher test-connectivity \
  --resource-group rg-network-lab \
  --source-resource vm-web \
  --dest-address mystorageaccount.dfs.core.windows.net \
  --dest-port 443

# Returns: hops, latency, whether connection succeeded, which rule blocked it
# This is the most comprehensive single-command test Azure offers
```

---

### Check Private Endpoint connection status

```bash
# Show all private endpoint connections for a storage account
az storage account show \
  --resource-group rg-network-lab \
  --name mystorageaccount \
  --query privateEndpointConnections \
  --output table

# Expected:
# Name                    PrivateLinkServiceConnectionState
# ----------------------  ----------------------------------
# pe-adls-dfs.connection  Approved
```

---

### Check DNS resolution from Azure CLI

```bash
# Resolve from Azure side (useful when you can't SSH into a VM)
az network private-endpoint dns-zone-group show \
  --resource-group rg-network-lab \
  --endpoint-name pe-adls-dfs \
  --name pe-dns-group

# Get the private IP of a Private Endpoint
az network private-endpoint show \
  --resource-group rg-network-lab \
  --name pe-adls-dfs \
  --query "customDnsConfigs[*].{FQDN:fqdn, IP:ipAddresses[0]}" \
  --output table

# Expected:
# FQDN                                          IP
# --------------------------------------------  ----------
# mystorageaccount.dfs.core.windows.net         10.0.3.4
```

---

## 10. Databricks-Specific Tests

> Run these in Databricks notebooks using `%sh` for shell commands.
> Replace `%sh` with a subprocess call in PySpark if needed.

---

### DNS test from inside a cluster

```python
# From a Databricks notebook cell:
%sh nslookup mystorageaccount.dfs.core.windows.net

# Expected when Private Endpoint is configured:
# Server:  168.63.129.16       ← Azure DNS (the internal Azure resolver)
# Address: 168.63.129.16#53
#
# Non-authoritative answer:
# Name:    mystorageaccount.dfs.core.windows.net
# Address: 10.0.3.4            ← ✅ Private IP (PE is working!)
#
# If you see 20.150.x.x here:
# ❌ The cluster's DNS is not using the Private DNS Zone
#    → Check if Private DNS Zone is linked to the cluster's VNet
```

---

### Check cluster's own IP address

```python
%sh ip addr | grep "inet "
# Expected for VNet-injected cluster:
# inet 10.5.2.4/26   → cluster VM is in YOUR subnet (dbr-private-subnet)

# From PySpark (no %sh):
import subprocess
result = subprocess.run(["ip", "addr"], capture_output=True, text=True)
print(result.stdout)
```

---

### TCP port test from inside a cluster

```python
# Test if Private Endpoint NIC port 443 is reachable from the cluster
%sh nc -zv 10.0.3.4 443
# Expected: Connection to 10.0.3.4 443 port [tcp/https] succeeded!

# Test by hostname (confirms DNS + connectivity together)
%sh nc -zv mystorageaccount.dfs.core.windows.net 443

# Test on-prem database port (only works if Private Network Gateway is set up)
%sh nc -zv 192.168.0.4 5432
# Expected: Connection to 192.168.0.4 5432 port [tcp/postgresql] succeeded!
# If timeout: Private Network Gateway not configured or gateway not running
```

---

### Traceroute from inside a cluster

```python
%sh traceroute -n mystorageaccount.dfs.core.windows.net
# Good result (PE working):
#  1  10.5.2.1   0.4 ms      ← subnet default gateway
#  2  10.0.3.4   1.0 ms      ← destination is private IP (PE NIC)
#
# Bad result (going public):
#  1  10.5.2.1   0.4 ms
#  2  * * *                  ← Azure backbone (ICMP blocked)
#  3  20.150.x.x  2.5 ms     ← ❌ public Azure IP reached

%sh traceroute -T -p 443 -n mystorageaccount.dfs.core.windows.net
# -T = TCP mode (less likely to be blocked by NSG)
```

---

### Check outbound IP (NAT / public IP of cluster)

```python
# From a cluster notebook, what is the cluster's outbound public IP?
%sh curl -s https://ifconfig.me
# If VNet-injected with NAT Gateway:      → returns NAT Gateway public IP
# If no NAT Gateway and has public IP:    → returns the VM's public IP
# If no public outbound:                  → times out (secure cluster connectivity)
```

---

### Verify ADLS access with actual Spark read

```python
# Full end-to-end test: DNS + auth + network + data read
ADLS_NAME = "mystorageaccount"

try:
    df = spark.read.csv(
        f"abfss://bronze@{ADLS_NAME}.dfs.core.windows.net/data/sample.csv",
        header=True
    )
    df.show()
    print(f"✅ SUCCESS: Read {df.count()} rows from ADLS via Private Endpoint")
except Exception as e:
    print(f"❌ FAILED: {e}")
    # Common errors:
    # "AuthorizationPermissionMismatch" → RBAC not set (SP missing role)
    # "Connection timed out"            → NSG blocking port 443 or no route to PE
    # "ResolutionError"                 → DNS resolving to public IP
    # "403 Forbidden"                   → ADLS firewall blocking (IP allowlist issue)
```

---

### Check Databricks cluster DNS config

```python
# What DNS servers is this cluster using?
%sh cat /etc/resolv.conf
# Expected inside Azure VNet:
# nameserver 168.63.129.16     ← Azure DNS (this is correct)
#
# If you see 8.8.8.8 or 1.1.1.1:
# ❌ Cluster is using external DNS → won't resolve Private DNS Zones
#    Fix: In VNet DNS settings, remove custom DNS or point to 168.63.129.16
```

---

### Run connectivity test script from notebook

```python
# Comprehensive one-shot connectivity test cell
import subprocess

def check(label, cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
    status = "✅" if result.returncode == 0 else "❌"
    print(f"{status} {label}")
    if result.stdout.strip():
        print(f"   {result.stdout.strip()[:200]}")
    if result.returncode != 0 and result.stderr.strip():
        print(f"   ERROR: {result.stderr.strip()[:200]}")
    print()

ADLS  = "mystorageaccount.dfs.core.windows.net"
PE_IP = "10.0.3.4"   # your PE private IP

check("Own IP",              "ip addr | grep 'inet ' | head -3")
check("DNS resolves ADLS",   f"nslookup {ADLS} | grep -E 'Address|Name'")
check("Port 443 to ADLS",    f"nc -zv -w 5 {ADLS} 443")
check("Outbound public IP",  "curl -s --max-time 5 https://ifconfig.me")
check("Azure DNS reachable", "nc -zv -w 3 168.63.129.16 53")
check("Traceroute to ADLS",  f"traceroute -n -m 5 {ADLS}")
```

---

## 11. Storage Account Connectivity Tests

### Check if ADLS responds (expect 403, not timeout)

```bash
# A 403 response means: reachable ✅ but not authenticated
# A timeout / connection refused means: NOT reachable ❌

curl -o /dev/null -s -w "%{http_code}" \
  https://mystorageaccount.blob.core.windows.net

# 403 → endpoint is reachable (auth blocked, not network)
# 000 → connection failed (NSG, firewall, or DNS going to public IP when public access disabled)
# 200 → reachable and anonymous access allowed (check your security settings!)
```

---

### Check Private Endpoint IP via DNS lookup

```bash
# Confirm storage resolves to PRIVATE IP from within VNet
nslookup mystorageaccount.dfs.core.windows.net 168.63.129.16
# Expected:
#   Address: 10.0.3.4   ← private IP (Private Endpoint)
# Not expected:
#   Address: 20.150.x.x ← public IP (PE missing or DNS zone not linked)
```

---

### AzCopy connectivity test (Storage diagnostic tool)

```bash
# Install AzCopy
wget https://aka.ms/downloadazcopy-v10-linux -O azcopy.tar.gz
tar -xvf azcopy.tar.gz
sudo mv azcopy_linux_amd64_*/azcopy /usr/local/bin/

# Login
azcopy login

# Test by listing a container
azcopy list "https://mystorageaccount.dfs.core.windows.net/bronze"
# Success: shows file list
# Connection refused / 403: check PE + RBAC
```

---

### Azure CLI storage test

```bash
# Try to list blobs (confirms both network path AND authentication)
az storage blob list \
  --account-name mystorageaccount \
  --container-name bronze \
  --auth-mode login \
  --output table

# "auth-mode login" = uses your current az login identity
# Success: table of blobs
# "AuthorizationPermissionMismatch": logged-in user missing RBAC
# "Connection refused" / "timeout": network issue (PE or firewall)
```

---

## 12. On-Prem / VPN Connectivity

### Test VPN tunnel is up (from Azure side)

```bash
# Check VPN Gateway connection status
az network vpn-connection show \
  --resource-group rg-network-lab \
  --name vpn-conn-to-onprem \
  --query "connectionStatus" \
  -o tsv
# Expected: Connected

# Check BGP peer state (if using BGP routing)
az network vnet-gateway list-bgp-peer-status \
  --resource-group rg-network-lab \
  --name vpn-gw-prod \
  --output table

# Check learned routes (what on-prem prefixes Azure learned)
az network vnet-gateway list-learned-routes \
  --resource-group rg-network-lab \
  --name vpn-gw-prod \
  --output table
# You should see your on-prem network CIDR (e.g. 192.168.0.0/24) here
```

---

### Test on-prem host reachability from Azure VM

```bash
# SSH into an Azure VM (in the connected VNet) and test:

# Can you ping the on-prem host? (may fail if ICMP blocked)
ping -c 3 192.168.0.4

# Can you reach the on-prem DB port?
nc -zv -w 5 192.168.0.4 5432    # PostgreSQL
nc -zv -w 5 192.168.0.5 1521    # Oracle
nc -zv -w 5 192.168.0.6 1433    # SQL Server

# Traceroute to see the path
traceroute -n 192.168.0.4
# Expected: should show the VPN Gateway hop (10.0.x.x) then on-prem IPs
```

---

### Test Databricks Private Network Gateway connectivity

```python
# From Databricks notebook (serverless or classic cluster)
# Test if gateway is routing traffic to on-prem

# First: check if the on-prem IP is reachable
%sh nc -zv -w 5 192.168.0.4 5432
# If Private Network Gateway is set up and running:
#   → Connection to 192.168.0.4 5432 port [tcp/postgresql] succeeded! ✅
# If not set up:
#   → nc: connect to 192.168.0.4 port 5432 (tcp) failed: No route to host ❌

# Full JDBC connection test
jdbc_url = "jdbc:postgresql://192.168.0.4:5432/labdb"
props = {"user": "labuser", "password": "LabP@ss2024", "driver": "org.postgresql.Driver"}

try:
    df = spark.read.jdbc(url=jdbc_url, table="sales", properties=props)
    df.show()
    print("✅ Connected to on-prem PostgreSQL via Private Network Gateway!")
except Exception as e:
    print(f"❌ Failed: {e}")
    # "No route to host"    → Gateway not configured, gateway VM offline, or NSG blocking
    # "Connection refused"  → Reached on-prem VM but PostgreSQL not listening on that port
    # "auth failed"         → Network is fine, wrong credentials
```

---

## 13. Quick Cheat Sheet

```
GOAL                                    COMMAND
─────────────────────────────────────── ──────────────────────────────────────────────────
Resolve hostname to IP                  nslookup <hostname>
Resolve with specific DNS server        nslookup <hostname> 168.63.129.16
Full DNS chain trace                    dig +trace <hostname>
Test if TCP port is open                nc -zv -w 5 <host> <port>
Test TCP (Windows)                      Test-NetConnection -ComputerName <h> -Port <p>
Check my outbound public IP             curl https://ifconfig.me
Trace the network path                  traceroute -n <host>
Trace via TCP (bypass ICMP blocks)      traceroute -T -p 443 -n <host>
Show my VM's private IP                 ip addr | grep "inet "
Show routing table                      ip route
What route does my traffic take?        ip route get <destination_ip>
Show effective NSG rules on VM          az network nic show-effective-nsg ...
IP flow verify (Azure NSG check)        az network watcher test-ip-flow ...
End-to-end Azure connectivity test      az network watcher test-connectivity ...
Check HTTPS endpoint (expect 403)       curl -o /dev/null -s -w "%{http_code}" <url>
Check PE private IP via DNS             nslookup <fqdn> 168.63.129.16
Show active TCP connections             ss -tn
Show listening ports                    ss -tlnp
Check cluster DNS config                %sh cat /etc/resolv.conf
Check cluster's own IP (from notebook)  %sh ip addr | grep "inet "
Test port from Databricks notebook      %sh nc -zv -w 5 <host> <port>
Traceroute from Databricks notebook     %sh traceroute -T -p 443 -n <host>
VPN connection status                   az network vpn-connection show ...
List PE connections on storage          az storage account show ... --query privateEndpointConnections
```

---

### Common Error → Root Cause Mapping

```
ERROR                               ROOT CAUSE                      FIX
──────────────────────────────────  ──────────────────────────────  ─────────────────────────────────────
Connection timed out                NSG blocking the port           Check effective NSG rules
                                    OR: no route to host            Check routing table / UDR
                                    OR: PE missing                  Re-create Private Endpoint

Connection refused                  Host reachable, port closed     Check if service is running on target
                                    OR: firewall on the OS          Check iptables / Windows Firewall

403 Forbidden                       Reachable ✅ but auth failed    Check RBAC role, SP credentials
                                                                     Check Managed Identity assignment

AuthorizationPermissionMismatch     Spark auth config wrong         Check Spark config OAuth settings
                                    OR: SP missing RBAC role

nslookup returns public IP (20.x)   Private DNS Zone not linked     Link zone to VNet
from inside VNet                    OR: VM using wrong DNS server   Set VNet DNS to 168.63.129.16
                                    OR: PE A record missing         Add A record to Private DNS Zone

ping fails, nc succeeds             ICMP blocked by NSG             Normal in Azure — ignore ping, use nc

No route to host (on-prem)          VPN tunnel down                 Check VPN Gateway status
                                    OR: on-prem route not learned   Check BGP learned routes
                                    OR: NSG blocking VPN traffic    Add NSG rules for VPN address space

Private Network Gateway timeout     Gateway VM offline              Check gateway status in Account Console
(serverless → on-prem fails)        OR: gateway NSG too restrictive Check outbound NSG rules on gateway subnet
                                    OR: GW not attached to workspace Check workspace networking settings
```

---

### DNS Resolution Chain for Private Endpoints

```
Inside Azure VNet:

  VM does: nslookup mystorageaccount.blob.core.windows.net
                │
                ▼
  168.63.129.16 (Azure DNS)
                │
                ▼ (checks Private DNS Zones linked to this VNet)
  privatelink.blob.core.windows.net zone
                │
                ▼ (A record: mystorageaccount → 10.0.3.4)
  Returns: 10.0.3.4  ✅ Private Endpoint IP


Outside VNet (or wrong DNS):

  VM does: nslookup mystorageaccount.blob.core.windows.net
                │
                ▼
  8.8.8.8 (Google DNS) — no knowledge of Private DNS Zones
                │
                ▼ (resolves via public Azure DNS)
  Returns: 20.150.x.x  ❌ Public IP

  Even if you have a Private Endpoint — if DNS returns the public IP,
  your traffic goes to the public endpoint and gets BLOCKED (if public access disabled).
  → ALWAYS ensure VMs/clusters use 168.63.129.16 as DNS!
```
