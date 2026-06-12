# Azure Networking — Hands-On Lab Guide (Step by Step)

> **How to use this file:**
> Each lab is self-contained. Do them in order — each one builds on the previous.
> Every step has: what to click/type → what you should see → why you're doing it.
> All labs use the FREE tier or cheapest options to minimise cost.
> **Remember to DELETE resources after each lab to avoid charges.**

---

## Prerequisites — Do This Once Before All Labs

### What you need
```
✅ Azure free account or Pay-As-You-Go subscription
   Sign up: https://azure.microsoft.com/free (gives $200 credit for 30 days)

✅ Azure CLI installed on your laptop
   Download: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli

✅ A terminal (PowerShell on Windows, Terminal on Mac/Linux)
```

### Login to Azure CLI — do this first
```bash
# Step 1: Open PowerShell / Terminal and login
az login
# A browser window opens → sign in with your Azure account
# You will see a JSON list of your subscriptions in the terminal

# Step 2: Check you are logged in correctly
az account show
# Expected output:
# {
#   "name": "My Azure Subscription",
#   "state": "Enabled",
#   "tenantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# }

# Step 3: If you have multiple subscriptions, set the right one
az account set --subscription "My Azure Subscription"
# No output = success

# Step 4: Set default location (East US used in all labs — cheapest)
az configure --defaults location=eastus
# No output = success
```

---

## LAB 1 — Create a VNet with Subnets (Foundation)

**Goal:** Create a Virtual Network with 3 subnets — web, database, and private endpoints.
**Time:** ~10 minutes
**Cost:** FREE

### Step 1 — Create a Resource Group
```bash
# A resource group is the container for all lab resources.
# Think of it as a folder.

az group create \
  --name rg-network-lab \
  --location eastus

# Expected output:
# {
#   "id": "/subscriptions/.../resourceGroups/rg-network-lab",
#   "location": "eastus",
#   "name": "rg-network-lab",
#   "properties": {
#     "provisioningState": "Succeeded"
#   }
# }

# ✅ "provisioningState": "Succeeded" means it worked.
```

### Step 2 — Create the Virtual Network
```bash
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-lab \
  --address-prefix 10.0.0.0/16 \
  --location eastus

# Expected output (shortened):
# {
#   "newVNet": {
#     "addressSpace": {
#       "addressPrefixes": ["10.0.0.0/16"]
#     },
#     "name": "vnet-lab",
#     "provisioningState": "Succeeded"
#   }
# }

# What this means:
# You now have a private network "10.0.0.0/16"
# That's 65,536 IP addresses (10.0.0.0 to 10.0.255.255)
# Nothing is inside yet — no subnets, no VMs
```

### Step 3 — Create 3 Subnets
```bash
# Subnet 1: For web servers (internet-facing)
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --address-prefix 10.0.1.0/24

# Expected: "provisioningState": "Succeeded"
# This subnet has IPs: 10.0.1.0 to 10.0.1.255 (256 total, 251 usable)

# Subnet 2: For databases (internal only)
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-db \
  --address-prefix 10.0.2.0/24

# Expected: "provisioningState": "Succeeded"

# Subnet 3: For Private Endpoints
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-pe \
  --address-prefix 10.0.3.0/24

# Expected: "provisioningState": "Succeeded"
```

### Step 4 — Verify what you created
```bash
# List all subnets in the VNet
az network vnet subnet list \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --output table

# Expected output:
# Name        AddressPrefix    ProvisioningState
# ----------  ---------------  -----------------
# subnet-web  10.0.1.0/24      Succeeded
# subnet-db   10.0.2.0/24      Succeeded
# subnet-pe   10.0.3.0/24      Succeeded

# ✅ All 3 subnets created. You have a VNet with 3 internal "floors".
```

### Step 5 — View in Azure Portal (optional visual check)
```
1. Go to: https://portal.azure.com
2. Search: "Virtual Networks"
3. Click: vnet-lab
4. Left menu → "Subnets"
5. You should see: subnet-web, subnet-db, subnet-pe listed

What you'll notice:
- Each subnet shows its address range
- "Available IPs" shows 251 (256 minus 5 Azure reserved)
- No NSG attached yet (we add that in Lab 2)
```

---

## LAB 2 — Create and Attach NSG Rules

**Goal:** Create a Network Security Group, add firewall rules, attach to subnets.
**Time:** ~15 minutes
**Cost:** FREE

### Step 1 — Create an NSG for the web subnet
```bash
az network nsg create \
  --resource-group rg-network-lab \
  --name nsg-web

# Expected output:
# {
#   "NewNSG": {
#     "name": "nsg-web",
#     "provisioningState": "Succeeded",
#     "securityRules": []          ← no custom rules yet
#     "defaultSecurityRules": [...]← Azure adds 3 default rules automatically
#   }
# }
```

### Step 2 — Check the default rules Azure adds automatically
```bash
az network nsg rule list \
  --resource-group rg-network-lab \
  --nsg-name nsg-web \
  --output table

# Expected output:
# Name                           Priority  Direction  Access  Protocol  SourcePortRange  DestinationPortRange  SourceAddressPrefix  DestinationAddressPrefix
# -----------------------------  --------  ---------  ------  --------  ---------------  --------------------  -------------------  -----------------------
# AllowVnetInBound               65000     Inbound    Allow   *         *                *                     VirtualNetwork       VirtualNetwork
# AllowAzureLoadBalancerInBound  65001     Inbound    Allow   *         *                *                     AzureLoadBalancer    *
# DenyAllInBound                 65500     Inbound    Deny    *         *                *                     *                    *
# AllowVnetOutBound              65000     Outbound   Allow   *         *                *                     VirtualNetwork       VirtualNetwork
# AllowInternetOutBound          65001     Outbound   Allow   *         *                *                     *                    Internet
# DenyAllOutBound                65500     Outbound   Deny    *         *                *                     *                    *

# What this means:
# INBOUND: By default — only VNet traffic is allowed. Internet is BLOCKED.
# OUTBOUND: By default — VNet and Internet are ALLOWED.
```

### Step 3 — Add a rule to allow HTTPS from the internet
```bash
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-web \
  --name Allow-HTTPS-Inbound \
  --priority 100 \
  --direction Inbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix Internet \
  --source-port-range "*" \
  --destination-address-prefix "*" \
  --destination-port-range 443

# Expected: "provisioningState": "Succeeded"

# What this does:
# Priority 100 (checked BEFORE the default DenyAll at 65500)
# Allows any internet IP (Internet tag) to reach port 443 (HTTPS)
# on any VM in this subnet
```

### Step 4 — Add a rule to allow HTTP (redirect to HTTPS in real apps)
```bash
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-web \
  --name Allow-HTTP-Inbound \
  --priority 110 \
  --direction Inbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix Internet \
  --source-port-range "*" \
  --destination-address-prefix "*" \
  --destination-port-range 80

# Expected: "provisioningState": "Succeeded"
```

### Step 5 — Create a separate NSG for the database subnet (more restrictive)
```bash
az network nsg create \
  --resource-group rg-network-lab \
  --name nsg-db

# Allow ONLY the web subnet to reach the DB on port 5432 (PostgreSQL)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-db \
  --name Allow-WebTier-PostgreSQL \
  --priority 100 \
  --direction Inbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix 10.0.1.0/24 \
  --source-port-range "*" \
  --destination-address-prefix "*" \
  --destination-port-range 5432

# DENY internet from reaching the DB (explicit, though DenyAll already does this)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-db \
  --name Deny-Internet-DB \
  --priority 200 \
  --direction Inbound \
  --access Deny \
  --protocol "*" \
  --source-address-prefix Internet \
  --source-port-range "*" \
  --destination-address-prefix "*" \
  --destination-port-range "*"

# Expected: both rules "provisioningState": "Succeeded"
```

### Step 6 — Attach NSGs to subnets
```bash
# Attach nsg-web to subnet-web
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --network-security-group nsg-web

# Expected: subnet JSON with "networkSecurityGroup": { "id": "...nsg-web" }

# Attach nsg-db to subnet-db
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-db \
  --network-security-group nsg-db

# Expected: subnet JSON with "networkSecurityGroup": { "id": "...nsg-db" }
```

### Step 7 — Verify NSG attachments
```bash
az network vnet subnet list \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --output table

# Expected output:
# Name        AddressPrefix    ProvisioningState  NetworkSecurityGroup
# ----------  ---------------  -----------------  --------------------
# subnet-web  10.0.1.0/24      Succeeded          nsg-web
# subnet-db   10.0.2.0/24      Succeeded          nsg-db
# subnet-pe   10.0.3.0/24      Succeeded          (empty — no NSG needed for PE subnet)
```

### Step 8 — Test the rules conceptually
```
After these NSG rules:

  Someone from internet → port 443 → VM in subnet-web    ✅ (rule Allow-HTTPS at priority 100)
  Someone from internet → port 80  → VM in subnet-web    ✅ (rule Allow-HTTP at priority 110)
  Someone from internet → port 22  → VM in subnet-web    ❌ (no allow rule, DenyAll at 65500 kicks in)
  Web VM (10.0.1.x) → port 5432   → DB VM in subnet-db  ✅ (rule Allow-WebTier at priority 100 on nsg-db)
  Internet          → port 5432   → DB VM in subnet-db   ❌ (Deny-Internet-DB at 200 + DenyAll at 65500)
```

---

## LAB 3 — Create Two VMs and Test Communication

**Goal:** Create one VM in subnet-web, one in subnet-db. Test they can communicate.
**Time:** ~20 minutes
**Cost:** ~$0.01–0.05 (VM run time, delete after lab)

### Step 1 — Add SSH allow rule to NSG (so you can connect to test)
```bash
# Allow SSH from YOUR IP only (more secure than allowing all internet)
# First, find YOUR public IP:
curl ifconfig.me
# Example output: 203.0.113.5   ← this is YOUR IP (yours will be different)

# Add SSH rule to web NSG (so you can SSH in for testing)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-web \
  --name Allow-SSH-MyIP \
  --priority 120 \
  --direction Inbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix "203.0.113.5"  \
  --source-port-range "*" \
  --destination-address-prefix "*" \
  --destination-port-range 22

# REPLACE 203.0.113.5 with YOUR actual IP from the curl command above
```

### Step 2 — Create a Public IP for the web VM
```bash
az network public-ip create \
  --resource-group rg-network-lab \
  --name pip-vm-web \
  --sku Standard \
  --allocation-method Static

# Expected output includes:
# "ipAddress": "20.x.x.x"   ← the public IP assigned to you (note this down)

# Save the IP:
az network public-ip show \
  --resource-group rg-network-lab \
  --name pip-vm-web \
  --query ipAddress \
  --output tsv
# Output: 20.x.x.x   ← SAVE THIS — you'll use it to SSH
```

### Step 3 — Create the web VM (Linux, smallest size)
```bash
az vm create \
  --resource-group rg-network-lab \
  --name vm-web \
  --image Ubuntu2204 \
  --size Standard_B1s \
  --vnet-name vnet-lab \
  --subnet subnet-web \
  --public-ip-address pip-vm-web \
  --nsg "" \
  --admin-username azureuser \
  --generate-ssh-keys

# This takes ~2 minutes.
# Expected output:
# {
#   "fqdns": "",
#   "id": "/subscriptions/.../virtualMachines/vm-web",
#   "privateIpAddress": "10.0.1.4",    ← private IP assigned from subnet-web
#   "publicIpAddress": "20.x.x.x",
#   "resourceGroup": "rg-network-lab"
# }

# KEY: privateIpAddress: 10.0.1.4  ← this is the VM's internal IP
# --nsg "" means don't create a new NSG (we already have nsg-web on the subnet)
# --generate-ssh-keys creates an SSH key pair in ~/.ssh/ automatically
```

### Step 4 — Create the DB VM (no public IP — internal only)
```bash
az vm create \
  --resource-group rg-network-lab \
  --name vm-db \
  --image Ubuntu2204 \
  --size Standard_B1s \
  --vnet-name vnet-lab \
  --subnet subnet-db \
  --public-ip-address "" \
  --nsg "" \
  --admin-username azureuser \
  --generate-ssh-keys

# Expected output:
# {
#   "privateIpAddress": "10.0.2.4",    ← DB VM is only reachable on private IP
#   "publicIpAddress": "",              ← NO public IP — internet can't reach it directly
# }

# This VM is ONLY reachable from inside the VNet (or via VPN).
```

### Step 5 — SSH into the web VM
```bash
# Get the public IP if you forgot it:
az network public-ip show \
  --resource-group rg-network-lab \
  --name pip-vm-web \
  --query ipAddress -o tsv

# SSH in:
ssh azureuser@20.x.x.x
# Replace 20.x.x.x with your actual public IP

# First time you'll see:
# "Are you sure you want to continue connecting (yes/no)?" → type: yes

# You should now be at a prompt like:
# azureuser@vm-web:~$
# ✅ You are now inside the Azure VM
```

### Step 6 — From web VM, ping and SSH into the DB VM
```bash
# You are now inside vm-web (azureuser@vm-web:~$)

# Test 1: Can web VM reach DB VM via private IP?
ping 10.0.2.4 -c 4

# Expected output:
# PING 10.0.2.4 (10.0.2.4) 56(84) bytes of data.
# 64 bytes from 10.0.2.4: icmp_seq=1 ttl=64 time=1.23 ms
# 64 bytes from 10.0.2.4: icmp_seq=2 ttl=64 time=0.98 ms
# ✅ ping works — same VNet, default routing allows it

# Test 2: Try SSH from web VM to DB VM
# (This will fail because NSG on subnet-db only allows port 5432, not SSH)
ssh azureuser@10.0.2.4
# Expected: Connection timed out (or connection refused)
# ✅ This is CORRECT behaviour — NSG is blocking SSH from web to db

# Test 3: Can web VM reach the internet?
curl -s https://ifconfig.me
# Expected: 20.x.x.x  (the public IP of the web VM)
# ✅ Outbound internet works via the Public IP

# Exit the VM when done
exit
```

### Step 7 — Test that DB VM is unreachable from internet
```bash
# From YOUR laptop (not inside Azure)

# Try to SSH directly to the DB VM private IP — will fail (no route from internet to private IP)
# That's expected — there's no public IP on vm-db

# You can verify the DB VM has no public IP:
az vm list-ip-addresses \
  --resource-group rg-network-lab \
  --name vm-db \
  --output table

# Expected:
# VirtualMachine    PrivateIPAddresses    PublicIPAddresses
# ----------------  --------------------  -----------------
# vm-db             10.0.2.4              (empty)

# ✅ DB VM only has private IP — internet cannot reach it directly
```

---

## LAB 4 — NAT Gateway (Outbound Internet Without Public IP)

**Goal:** Remove the public IP from vm-web, add a NAT Gateway so the VM can still reach the internet with a stable outbound IP.
**Time:** ~15 minutes
**Cost:** NAT Gateway ~$0.045/hour (~$0.05 for this lab)

### Step 1 — Create a Public IP for NAT Gateway
```bash
az network public-ip create \
  --resource-group rg-network-lab \
  --name pip-nat-gateway \
  --sku Standard \
  --allocation-method Static \
  --zone 1

# Expected: "ipAddress": "52.x.x.x"
# Note this IP — it will be the outbound IP for ALL VMs in the subnet
```

### Step 2 — Create the NAT Gateway
```bash
az network nat gateway create \
  --resource-group rg-network-lab \
  --name nat-gw-lab \
  --public-ip-addresses pip-nat-gateway \
  --idle-timeout 10

# Takes ~1 minute
# Expected: "provisioningState": "Succeeded"
```

### Step 3 — Attach NAT Gateway to subnet-web
```bash
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --nat-gateway nat-gw-lab

# Expected: subnet JSON with "natGateway": { "id": "...nat-gw-lab" }
```

### Step 4 — Detach the public IP from vm-web (simulate no public IP VM)
```bash
# Get the NIC name of vm-web
az vm show \
  --resource-group rg-network-lab \
  --name vm-web \
  --query networkProfile.networkInterfaces[0].id \
  --output tsv
# Returns something like: /subscriptions/.../networkInterfaces/vm-webVMNic

# Dissociate the public IP from the NIC
az network nic ip-config update \
  --resource-group rg-network-lab \
  --nic-name vm-webVMNic \
  --name ipconfig1 \
  --remove publicIpAddress

# Expected: NIC JSON without publicIpAddress
```

### Step 5 — SSH into vm-web via its new public IP... wait, it has no public IP now!
```bash
# Since we removed the public IP, we need another way to connect.
# For this lab, let's re-add a temporary Public IP just to test,
# OR use Azure Cloud Shell (browser-based terminal inside Azure Portal)

# OPTION — Use Azure Cloud Shell (easiest):
# 1. Go to portal.azure.com
# 2. Click the ">_" icon in the top bar (Cloud Shell)
# 3. Choose "Bash"
# 4. Cloud Shell runs INSIDE Azure, so it can reach Azure VMs

# From Cloud Shell, SSH to the VM's PRIVATE IP:
ssh -i ~/.ssh/id_rsa azureuser@10.0.1.4

# From inside vm-web, check outbound IP:
curl -s https://ifconfig.me
# Expected: 52.x.x.x  (the NAT Gateway public IP, NOT the VM's own IP)
# ✅ VM uses NAT Gateway for outbound — its own IP is completely private
```

### Step 6 — Verify: All VMs in subnet-web share the same outbound IP
```bash
# From Cloud Shell, SSH to vm-db (via its private IP, using vm-web as jump host)
# Actually, vm-db is in subnet-db which doesn't have NAT Gateway.
# To add NAT to subnet-db:
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-db \
  --nat-gateway nat-gw-lab

# Now BOTH subnets use the same NAT Gateway
# → Both VMs appear as 52.x.x.x to the outside world
# → Neither VM's real internal IP is ever exposed

# ✅ Key benefit: You can allowlist 52.x.x.x in external firewalls (e.g., ADLS storage firewall)
#    and ALL your VMs in both subnets are covered with ONE IP.
```

---

## LAB 5 — VNet Peering (Two VNets Talk to Each Other)

**Goal:** Create a second VNet with a VM. Peer both VNets so VMs in different VNets can communicate.
**Time:** ~20 minutes
**Cost:** ~$0.01 (VM + tiny data transfer)

### Step 1 — Create a second VNet and VM
```bash
# Create second VNet (DIFFERENT address space — must not overlap with 10.0.0.0/16)
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-lab2 \
  --address-prefix 10.1.0.0/16 \
  --location eastus

# Create a subnet in vnet-lab2
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab2 \
  --name subnet-app \
  --address-prefix 10.1.1.0/24

# Create a VM in vnet-lab2
az vm create \
  --resource-group rg-network-lab \
  --name vm-app \
  --image Ubuntu2204 \
  --size Standard_B1s \
  --vnet-name vnet-lab2 \
  --subnet subnet-app \
  --public-ip-address "" \
  --nsg "" \
  --admin-username azureuser \
  --generate-ssh-keys

# Expected: "privateIpAddress": "10.1.1.4"
```

### Step 2 — Try to ping BEFORE peering (should FAIL)
```bash
# From Azure Cloud Shell, SSH into vm-web (10.0.1.4)
ssh azureuser@10.0.1.4

# Try to reach vm-app in vnet-lab2
ping 10.1.1.4 -c 3

# Expected output:
# PING 10.1.1.4 (10.1.1.4) 56(84) bytes of data.
# (no response — times out)
# --- 10.1.1.4 ping statistics ---
# 3 packets transmitted, 0 received, 100% packet loss
# ❌ Cannot reach vnet-lab2 — no route exists yet

exit
```

### Step 3 — Create VNet Peering (BOTH directions required)
```bash
# Get the resource IDs of both VNets
VNET1_ID=$(az network vnet show \
  --resource-group rg-network-lab \
  --name vnet-lab \
  --query id -o tsv)

VNET2_ID=$(az network vnet show \
  --resource-group rg-network-lab \
  --name vnet-lab2 \
  --query id -o tsv)

echo "VNet1 ID: $VNET1_ID"
echo "VNet2 ID: $VNET2_ID"
# These long resource IDs are needed for the peering commands

# Create peering FROM vnet-lab TO vnet-lab2
az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-lab1-to-lab2 \
  --vnet-name vnet-lab \
  --remote-vnet $VNET2_ID \
  --allow-vnet-access

# Expected: "peeringState": "Initiated"
# (only ONE side configured so far — state is Initiated, not yet Connected)

# Create peering FROM vnet-lab2 TO vnet-lab
az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-lab2-to-lab1 \
  --vnet-name vnet-lab2 \
  --remote-vnet $VNET1_ID \
  --allow-vnet-access

# Expected: "peeringState": "Connected"
# ✅ Both sides configured — state is now Connected
```

### Step 4 — Verify peering status
```bash
az network vnet peering list \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --output table

# Expected:
# Name               PeeringState    AllowVnetAccess    RemoteAddressSpace
# -----------------  --------------  -----------------  -------------------
# peer-lab1-to-lab2  Connected       True               10.1.0.0/16

# ✅ PeeringState = Connected means traffic can flow
```

### Step 5 — Test communication AFTER peering (should SUCCEED)
```bash
# SSH into vm-web again (from Cloud Shell or via public IP if you still have one)
ssh azureuser@10.0.1.4   # from Cloud Shell

# Try to reach vm-app in vnet-lab2
ping 10.1.1.4 -c 4

# Expected output:
# PING 10.1.1.4 (10.1.1.4) 56(84) bytes of data.
# 64 bytes from 10.1.1.4: icmp_seq=1 ttl=62 time=1.45 ms
# 64 bytes from 10.1.1.4: icmp_seq=2 ttl=62 time=1.22 ms
# 4 packets transmitted, 4 received, 0% packet loss
# ✅ Peering works — vm-web can now reach vm-app across VNets

# Note the ttl=62 (not 64) — traffic crossed 2 network hops (the peering link)
exit
```

### Step 6 — Prove non-transitivity
```bash
# Create a 3rd VNet peered only with vnet-lab2
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-lab3 \
  --address-prefix 10.2.0.0/16

az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab3 \
  --name subnet-c \
  --address-prefix 10.2.1.0/24

az vm create \
  --resource-group rg-network-lab \
  --name vm-c \
  --image Ubuntu2204 \
  --size Standard_B1s \
  --vnet-name vnet-lab3 \
  --subnet subnet-c \
  --public-ip-address "" \
  --nsg "" \
  --admin-username azureuser \
  --generate-ssh-keys

# Peer lab2 ↔ lab3
VNET3_ID=$(az network vnet show -g rg-network-lab -n vnet-lab3 --query id -o tsv)

az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-lab2-to-lab3 \
  --vnet-name vnet-lab2 \
  --remote-vnet $VNET3_ID \
  --allow-vnet-access

az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-lab3-to-lab2 \
  --vnet-name vnet-lab3 \
  --remote-vnet $VNET2_ID \
  --allow-vnet-access

# Current peering state:
# vnet-lab ↔ vnet-lab2  (peered)
# vnet-lab2 ↔ vnet-lab3 (peered)
# Question: Can vm-web (10.0.1.4) reach vm-c (10.2.1.4)?

# SSH into vm-web and try:
ssh azureuser@10.0.1.4  # from Cloud Shell
ping 10.2.1.4 -c 3

# Expected:
# 3 packets transmitted, 0 received, 100% packet loss
# ❌ Cannot reach vnet-lab3 — peering is NOT transitive
# vnet-lab → vnet-lab2 → vnet-lab3 does NOT give vnet-lab access to vnet-lab3

exit
```

---

## LAB 6 — Private Endpoint for Azure Storage

**Goal:** Create an Azure Storage account, create a Private Endpoint so the VM accesses it on a private IP (not public IP), and disable public access.
**Time:** ~20 minutes
**Cost:** Storage ~$0.02/month, Private Endpoint ~$0.01/hour

### Step 1 — Create a Storage Account
```bash
# Storage account names must be globally unique (lowercase letters + numbers only)
# Replace "labstorage12345" with something unique
STORAGE_NAME="labstorage$(date +%s | tail -c 5)"
echo "Storage name: $STORAGE_NAME"   # save this!

az storage account create \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --sku Standard_LRS \
  --kind StorageV2 \
  --location eastus

# Expected: "provisioningState": "Succeeded"
```

### Step 2 — Upload a test file to prove access works publicly first
```bash
# Get the storage connection string
CONN_STR=$(az storage account show-connection-string \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --query connectionString -o tsv)

# Create a container
az storage container create \
  --name testcontainer \
  --connection-string $CONN_STR

# Upload a test file
echo "Hello from Azure Storage" > test.txt
az storage blob upload \
  --container-name testcontainer \
  --name test.txt \
  --file test.txt \
  --connection-string $CONN_STR

# Expected: "etag": "0x...", "lastModified": "..."
# ✅ File uploaded to public storage
```

### Step 3 — Check current public access is working
```bash
# Try to access from your laptop (public internet) — should work now
az storage blob download \
  --container-name testcontainer \
  --name test.txt \
  --file downloaded.txt \
  --connection-string $CONN_STR

cat downloaded.txt
# Expected: Hello from Azure Storage
# ✅ Public access working (no restriction yet)
```

### Step 4 — Create a Private Endpoint for the storage account
```bash
# Get the Storage Account resource ID
STORAGE_ID=$(az storage account show \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --query id -o tsv)

echo "Storage ID: $STORAGE_ID"

# Create Private Endpoint in pe-subnet (10.0.3.0/24)
az network private-endpoint create \
  --resource-group rg-network-lab \
  --name pe-storage-blob \
  --vnet-name vnet-lab \
  --subnet subnet-pe \
  --private-connection-resource-id $STORAGE_ID \
  --group-id blob \
  --connection-name conn-storage-blob

# Takes ~1 minute
# Expected: "provisioningState": "Succeeded"
# --group-id blob = the Blob storage sub-resource
# (use 'dfs' for ADLS Gen2 Data Lake Storage)
```

### Step 5 — Find the private IP assigned to the PE
```bash
az network private-endpoint show \
  --resource-group rg-network-lab \
  --name pe-storage-blob \
  --query customDnsConfigs \
  --output table

# Expected:
# Fqdn                                        IpAddresses
# ------------------------------------------  -----------
# labstorage12345.blob.core.windows.net       10.0.3.4

# ✅ The storage account now has a private IP (10.0.3.4) inside your VNet!
# Azure created a NIC in your pe-subnet with this IP.

# Also verify the NIC was created:
az network nic list \
  --resource-group rg-network-lab \
  --query "[?contains(name,'pe-storage')].[name,ipConfigurations[0].privateIpAddress]" \
  --output table
# Expected: pe-storage-blob.nic.xxxx   10.0.3.4
```

### Step 6 — Create Private DNS Zone
```bash
# Without this, DNS resolves storage to PUBLIC IP even though PE exists

az network private-dns zone create \
  --resource-group rg-network-lab \
  --name "privatelink.blob.core.windows.net"

# Expected: "provisioningState": "Succeeded"

# Link the DNS zone to the VNet
az network private-dns link vnet create \
  --resource-group rg-network-lab \
  --zone-name "privatelink.blob.core.windows.net" \
  --name dns-link-vnet-lab \
  --virtual-network vnet-lab \
  --registration-enabled false

# Expected: "provisioningState": "Succeeded"

# Add a DNS A record pointing the storage FQDN to the PE private IP
az network private-dns record-set a add-record \
  --resource-group rg-network-lab \
  --zone-name "privatelink.blob.core.windows.net" \
  --record-set-name $STORAGE_NAME \
  --ipv4-address 10.0.3.4

# Expected: Record set with name=labstorage12345, aRecords=[10.0.3.4]
```

### Step 7 — Now disable public access on the storage account
```bash
az storage account update \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --public-network-access Disabled

# Expected: "publicNetworkAccess": "Disabled"

# Test from YOUR LAPTOP (internet) — should now FAIL
az storage blob download \
  --container-name testcontainer \
  --name test.txt \
  --file downloaded2.txt \
  --connection-string $CONN_STR

# Expected error:
# (AuthorizationFailure) This request is not authorized to perform this operation.
# OR: (PublicAccessNotPermitted) Public access is not permitted
# ✅ Your laptop can no longer access storage — public access disabled
```

### Step 8 — Access storage from INSIDE the VNet (via private endpoint)
```bash
# SSH into vm-web (which is inside vnet-lab)
# From Azure Cloud Shell:
ssh azureuser@10.0.1.4

# Check what IP DNS resolves the storage to
nslookup $STORAGE_NAME.blob.core.windows.net
# OR
host $STORAGE_NAME.blob.core.windows.net

# Expected output:
# Name: labstorage12345.privatelink.blob.core.windows.net
# Address: 10.0.3.4   ← PRIVATE IP! Not the public IP.
# ✅ DNS returns the private endpoint IP when queried from inside the VNet

# Install azure-cli on the VM to test download
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login (use --use-device-code for headless)
az login --use-device-code
# Follow the instructions — open the URL and enter the code

# Download the file from storage (via private endpoint)
az storage blob download \
  --account-name $STORAGE_NAME \
  --container-name testcontainer \
  --name test.txt \
  --file /tmp/downloaded.txt \
  --auth-mode login

cat /tmp/downloaded.txt
# Expected: Hello from Azure Storage
# ✅ VM inside VNet can access storage via the private endpoint (10.0.3.4)

exit
```

---

## LAB 7 — VNet Peering with Route Table (Force Traffic Through Hub)

**Goal:** Demonstrate that peering is non-transitive, then add a Route Table to forward traffic through an intermediate hub — the basis of the Hub-Spoke model.
**Time:** ~25 minutes
**Cost:** ~$0.01 (VM run time)

### Step 1 — Understand the current setup
```
Current state after Lab 5:
  vnet-lab  (10.0.0.0/16) ↔ vnet-lab2 (10.1.0.0/16)  — peered
  vnet-lab2 (10.1.0.0/16) ↔ vnet-lab3 (10.2.0.0/16)  — peered

  vm-web  (10.0.1.4) can reach vm-app  (10.1.1.4) ✅
  vm-app  (10.1.1.4) can reach vm-c    (10.2.1.4) ✅
  vm-web  (10.0.1.4) CANNOT reach vm-c (10.2.1.4) ❌ (non-transitive)

Goal: Make vm-web able to reach vm-c by routing traffic through vm-app
      (simulating a hub-spoke where the hub routes traffic)
```

### Step 2 — Enable IP forwarding on vm-app (the "hub" VM)
```bash
# First, get the NIC name of vm-app
NIC_APP=$(az vm show \
  --resource-group rg-network-lab \
  --name vm-app \
  --query networkProfile.networkInterfaces[0].id \
  --output tsv | sed 's|.*/||')

echo "NIC name: $NIC_APP"

# Enable IP forwarding on the NIC
# (Without this, the OS drops packets not destined for its own IP)
az network nic update \
  --resource-group rg-network-lab \
  --name $NIC_APP \
  --ip-forwarding true

# Expected: "enableIpForwarding": true
```

### Step 3 — Enable IP forwarding on the OS inside vm-app
```bash
# SSH into vm-app (10.1.1.4) from Cloud Shell
ssh azureuser@10.1.1.4

# Enable IP forwarding in the Linux kernel
sudo sysctl -w net.ipv4.ip_forward=1

# Make it permanent (survives reboots)
echo "net.ipv4.ip_forward = 1" | sudo tee -a /etc/sysctl.conf

# Verify
cat /proc/sys/net/ipv4/ip_forward
# Expected: 1

exit
```

### Step 4 — Create a Route Table for vnet-lab
```bash
# This route table says: "to reach 10.2.0.0/16 (vnet-lab3), go via vm-app (10.1.1.4)"
az network route-table create \
  --resource-group rg-network-lab \
  --name rt-lab-to-lab3

# Add a route: traffic to vnet-lab3 (10.2.0.0/16) → send to vm-app (10.1.1.4)
az network route-table route create \
  --resource-group rg-network-lab \
  --route-table-name rt-lab-to-lab3 \
  --name route-to-vnetlab3 \
  --address-prefix 10.2.0.0/16 \
  --next-hop-type VirtualAppliance \
  --next-hop-ip-address 10.1.1.4

# Expected: "provisioningState": "Succeeded"
# VirtualAppliance = send to a specific IP (our vm-app as router)
```

### Step 5 — Attach the Route Table to subnet-web
```bash
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --route-table rt-lab-to-lab3

# Expected: subnet JSON includes "routeTable": { "id": "...rt-lab-to-lab3" }
```

### Step 6 — Enable peering to use remote gateway / allow forwarded traffic
```bash
# Update the peering to allow forwarded traffic
# (Required so vm-app can forward packets from vnet-lab to vnet-lab3)
az network vnet peering update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name peer-lab1-to-lab2 \
  --set allowForwardedTraffic=true

az network vnet peering update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab2 \
  --name peer-lab2-to-lab1 \
  --set allowForwardedTraffic=true

az network vnet peering update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab2 \
  --name peer-lab2-to-lab3 \
  --set allowForwardedTraffic=true

az network vnet peering update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab3 \
  --name peer-lab3-to-lab2 \
  --set allowForwardedTraffic=true
```

### Step 7 — Test connectivity now (vnet-lab → vnet-lab3 via vm-app)
```bash
# From Cloud Shell, SSH into vm-web
ssh azureuser@10.0.1.4

# Try to reach vm-c (10.2.1.4) now
ping 10.2.1.4 -c 4

# Expected (if IP forwarding is working):
# 64 bytes from 10.2.1.4: icmp_seq=1 ttl=62 time=2.1 ms
# ✅ Traffic is now flowing: vm-web → vm-app (10.1.1.4) → vm-c (10.2.1.4)
#    The route table forced traffic through vm-app, which forwarded it on.

# Trace the route to confirm
traceroute 10.2.1.4
# Expected hops:
# 1  10.0.0.1  (default gateway of subnet-web)
# 2  10.1.1.4  (vm-app — our "router")
# 3  10.2.1.4  (vm-c — destination)

exit
```

---

## LAB 8 — DNS and Private DNS Zone Verification

**Goal:** Understand how Azure DNS works, how Private DNS Zones override public DNS, and verify the resolution difference from inside vs outside the VNet.
**Time:** ~15 minutes
**Cost:** FREE

### Step 1 — Check DNS from OUTSIDE Azure (your laptop)
```bash
# From YOUR laptop terminal (not Azure Cloud Shell)

# Resolve the storage account FQDN from the internet
nslookup $STORAGE_NAME.blob.core.windows.net 8.8.8.8
# (8.8.8.8 = Google DNS — public resolver)

# Expected output:
# Server:   8.8.8.8
# Non-authoritative answer:
# Name:  blob.xyz.store.core.windows.net
# Addresses:  20.150.x.x   ← PUBLIC IP of Azure Storage
#             20.150.y.y

# ✅ From internet, DNS returns the PUBLIC IP
# Your laptop sees the public IP and tries to connect to it.
# But since we disabled public access in Lab 6, it would fail.
```

### Step 2 — Check DNS from INSIDE Azure (vm-web)
```bash
# From Azure Cloud Shell
ssh azureuser@10.0.1.4   # SSH into vm-web

# Resolve same FQDN from inside the VNet
nslookup $STORAGE_NAME.blob.core.windows.net

# Expected output:
# Server:   168.63.129.16     ← Azure's internal DNS resolver
# Address:  168.63.129.16#53
#
# Non-authoritative answer:
# $STORAGE_NAME.blob.core.windows.net canonical name = $STORAGE_NAME.privatelink.blob.core.windows.net
# Name:  $STORAGE_NAME.privatelink.blob.core.windows.net
# Address: 10.0.3.4   ← PRIVATE IP (Private Endpoint in pe-subnet)

# ✅ From inside VNet, DNS returns the PRIVATE IP (10.0.3.4)
# Traffic goes to the Private Endpoint NIC inside your VNet, not public internet.

exit
```

### Step 3 — See the CNAME chain that makes this work
```bash
# From vm-web (inside Azure VNet)
# Full DNS trace:
dig $STORAGE_NAME.blob.core.windows.net

# Expected output (abridged):
# QUESTION SECTION:
# labstorage12345.blob.core.windows.net.  IN A
#
# ANSWER SECTION:
# labstorage12345.blob.core.windows.net.  CNAME  labstorage12345.privatelink.blob.core.windows.net.
# labstorage12345.privatelink.blob.core.windows.net.  A  10.0.3.4
#
# SERVER: 168.63.129.16

# What happened:
# 1. VM asks Azure DNS (168.63.129.16) for labstorage12345.blob.core.windows.net
# 2. Azure DNS sees there's a CNAME → labstorage12345.privatelink.blob.core.windows.net
# 3. Azure DNS checks Private DNS Zone "privatelink.blob.core.windows.net" (linked to this VNet)
# 4. Finds record: labstorage12345 → 10.0.3.4
# 5. Returns 10.0.3.4 to the VM
# 6. VM connects to 10.0.3.4 (Private Endpoint) — traffic stays in VNet
```

### Step 4 — Create a custom DNS record (simulate internal service discovery)
```bash
# Create a private DNS zone for internal services
az network private-dns zone create \
  --resource-group rg-network-lab \
  --name "internal.lab.local"

# Link to vnet-lab
az network private-dns link vnet create \
  --resource-group rg-network-lab \
  --zone-name "internal.lab.local" \
  --name link-internal-lab \
  --virtual-network vnet-lab \
  --registration-enabled false

# Add a record: database.internal.lab.local → 10.0.2.4 (vm-db's private IP)
az network private-dns record-set a add-record \
  --resource-group rg-network-lab \
  --zone-name "internal.lab.local" \
  --record-set-name database \
  --ipv4-address 10.0.2.4

# Add a record: webserver.internal.lab.local → 10.0.1.4 (vm-web)
az network private-dns record-set a add-record \
  --resource-group rg-network-lab \
  --zone-name "internal.lab.local" \
  --record-set-name webserver \
  --ipv4-address 10.0.1.4
```

### Step 5 — Test custom DNS from inside the VNet
```bash
# SSH into vm-web
ssh azureuser@10.0.1.4

# Resolve using your custom internal DNS
nslookup database.internal.lab.local
# Expected: Address: 10.0.2.4  ✅

nslookup webserver.internal.lab.local
# Expected: Address: 10.0.1.4  ✅

# Connect to DB using the hostname instead of IP
# (simulates how applications use DNS names, not hardcoded IPs)
ping database.internal.lab.local -c 3
# Expected: PING database.internal.lab.local (10.0.2.4) — replies!

exit
```

---

## LAB 9 — Storage Account Firewall Scenarios

**Goal:** Practice different storage firewall settings — from open to locked-down with selected VNets.
**Time:** ~10 minutes
**Cost:** FREE (uses existing storage from Lab 6)

### Scenario A — Allow from selected VNet only (Service Endpoint approach)
```bash
# First, re-enable public access (to reset from Lab 6)
az storage account update \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --public-network-access Enabled

# Enable Service Endpoint for Storage on subnet-web
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --service-endpoints Microsoft.Storage

# Expected: serviceEndpoints includes "Microsoft.Storage"

# Now add a VNET rule to the storage firewall
SUBNET_WEB_ID=$(az network vnet subnet show \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-web \
  --query id -o tsv)

az storage account network-rule add \
  --resource-group rg-network-lab \
  --account-name $STORAGE_NAME \
  --vnet-name vnet-lab \
  --subnet subnet-web

# Change storage firewall to "deny all except selected VNets"
az storage account update \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --default-action Deny \
  --bypass AzureServices

# Expected: "networkRuleSet.defaultAction": "Deny"

# Result:
# From internet (your laptop):        ❌ Denied
# From subnet-web (vm-web):           ✅ Allowed (service endpoint + VNet rule)
# From subnet-db (vm-db):             ❌ Denied (no VNet rule for subnet-db)
# From Azure services (Backup, ADF):  ✅ Allowed (--bypass AzureServices)
```

### Scenario B — Allow specific IP (your laptop's public IP)
```bash
MY_IP=$(curl -s https://ifconfig.me)
echo "My IP: $MY_IP"

az storage account network-rule add \
  --resource-group rg-network-lab \
  --account-name $STORAGE_NAME \
  --ip-address $MY_IP

# Now YOUR laptop can access storage (IP allowlisted)
# Test:
az storage blob download \
  --account-name $STORAGE_NAME \
  --container-name testcontainer \
  --name test.txt \
  --file /tmp/from-laptop.txt \
  --auth-mode login

# Expected: Download succeeds
# ✅ Your specific IP is now allowed

# Remove the IP rule when done
az storage account network-rule remove \
  --resource-group rg-network-lab \
  --account-name $STORAGE_NAME \
  --ip-address $MY_IP
```

### Scenario C — Fully locked down (Private Endpoint only)
```bash
# Disable ALL public access (only PE works)
az storage account update \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --public-network-access Disabled

# Verify:
az storage account show \
  --resource-group rg-network-lab \
  --name $STORAGE_NAME \
  --query networkRuleSet \
  --output table

# Expected:
# Bypass       DefaultAction
# -----------  -------------
# AzureServices  Deny

# And publicNetworkAccess = Disabled

# Now ONLY the Private Endpoint (10.0.3.4) works.
# All public traffic, all service endpoint traffic = blocked.
```

---

## LAB 10 — Clean Up (Delete Everything)

**Goal:** Delete all lab resources to avoid charges.
**Time:** ~5 minutes

### Option A — Delete everything at once (easiest)
```bash
# Delete the entire resource group — this deletes ALL resources inside it
az group delete \
  --name rg-network-lab \
  --yes \
  --no-wait

# --yes = don't ask for confirmation
# --no-wait = returns immediately (deletion continues in background)

# Check deletion status:
az group show --name rg-network-lab --query properties.provisioningState -o tsv
# Expected: "Deleting" (takes 5-10 minutes)
# Then: ResourceGroupNotFound (deletion complete)
```

### Option B — Delete selectively (if you want to keep some resources)
```bash
# Delete VMs first (most expensive)
az vm delete --resource-group rg-network-lab --name vm-web --yes --no-wait
az vm delete --resource-group rg-network-lab --name vm-db --yes --no-wait
az vm delete --resource-group rg-network-lab --name vm-app --yes --no-wait
az vm delete --resource-group rg-network-lab --name vm-c --yes --no-wait

# Delete Public IPs
az network public-ip delete --resource-group rg-network-lab --name pip-vm-web
az network public-ip delete --resource-group rg-network-lab --name pip-nat-gateway

# Delete NAT Gateway
az network nat gateway delete --resource-group rg-network-lab --name nat-gw-lab

# Delete Private Endpoints
az network private-endpoint delete --resource-group rg-network-lab --name pe-storage-blob

# Delete Storage Account
az storage account delete --resource-group rg-network-lab --name $STORAGE_NAME --yes

# Delete VNets (and all their peerings, subnets, NSGs attached to them)
az network vnet delete --resource-group rg-network-lab --name vnet-lab
az network vnet delete --resource-group rg-network-lab --name vnet-lab2
az network vnet delete --resource-group rg-network-lab --name vnet-lab3

# Finally delete the resource group (now empty)
az group delete --resource-group rg-network-lab --yes
```

### Verify everything is deleted
```bash
az group list --query "[?name=='rg-network-lab']" --output table
# Expected: (empty output = resource group deleted)

# Check your bill:
# Azure Portal → Cost Management → Cost Analysis
# Filter by resource group = rg-network-lab
# You should see only a few cents charged for this entire lab
```

---

## Quick Reference — Commands You Used in These Labs

```bash
# RESOURCE GROUP
az group create --name NAME --location eastus
az group delete --name NAME --yes

# VNET
az network vnet create --name NAME --address-prefix 10.0.0.0/16
az network vnet show   --name NAME --query id -o tsv
az network vnet delete --name NAME

# SUBNET
az network vnet subnet create --vnet-name VNET --name NAME --address-prefix 10.0.1.0/24
az network vnet subnet update --vnet-name VNET --name NAME --network-security-group NSG
az network vnet subnet update --vnet-name VNET --name NAME --nat-gateway NAT_GW
az network vnet subnet update --vnet-name VNET --name NAME --route-table RT_NAME
az network vnet subnet list   --vnet-name VNET --output table

# NSG
az network nsg create       --name NAME
az network nsg rule create  --nsg-name NAME --name RULE --priority 100 --direction Inbound/Outbound --access Allow/Deny --protocol Tcp --source-address-prefix IP --destination-port-range PORT
az network nsg rule list    --nsg-name NAME --output table

# PUBLIC IP
az network public-ip create --name NAME --sku Standard --allocation-method Static
az network public-ip show   --name NAME --query ipAddress -o tsv
az network public-ip delete --name NAME

# NAT GATEWAY
az network nat gateway create --name NAME --public-ip-addresses PIP_NAME
az network nat gateway delete --name NAME

# VNET PEERING
az network vnet peering create --vnet-name VNET --name PEER_NAME --remote-vnet VNET_ID --allow-vnet-access
az network vnet peering list   --vnet-name VNET --output table
az network vnet peering update --vnet-name VNET --name PEER_NAME --set allowForwardedTraffic=true

# ROUTE TABLE
az network route-table create       --name RT_NAME
az network route-table route create --route-table-name RT_NAME --name ROUTE --address-prefix CIDR --next-hop-type VirtualAppliance --next-hop-ip-address IP

# PRIVATE ENDPOINT
az network private-endpoint create --name NAME --vnet-name VNET --subnet SUBNET --private-connection-resource-id RESOURCE_ID --group-id blob/dfs/vault --connection-name CONN_NAME
az network private-endpoint show   --name NAME --query customDnsConfigs

# PRIVATE DNS ZONE
az network private-dns zone create      --name "privatelink.blob.core.windows.net"
az network private-dns link vnet create --zone-name ZONE --name LINK_NAME --virtual-network VNET --registration-enabled false
az network private-dns record-set a add-record --zone-name ZONE --record-set-name NAME --ipv4-address 10.0.3.4

# VM
az vm create  --name NAME --image Ubuntu2204 --size Standard_B1s --vnet-name VNET --subnet SUBNET --public-ip-address PIP --nsg "" --admin-username azureuser --generate-ssh-keys
az vm delete  --name NAME --yes
az vm list-ip-addresses --name NAME --output table

# STORAGE
az storage account create --name NAME --sku Standard_LRS --kind StorageV2
az storage account update --name NAME --public-network-access Disabled/Enabled
az storage account update --name NAME --default-action Deny/Allow
az storage account network-rule add --account-name NAME --ip-address IP
az storage account network-rule add --account-name NAME --vnet-name VNET --subnet SUBNET
```

---

## Troubleshooting — Common Issues in These Labs

```
ISSUE: "ping: vm-web pings vm-db but no reply"
FIX:   NSG on target subnet is blocking ICMP.
       Add NSG rule: Allow ICMP from source subnet.

ISSUE: "az vm create fails with 'NsgNotFound'"
FIX:   Don't use --nsg with the name of an NSG that doesn't exist.
       Use --nsg "" to skip NIC-level NSG (rely on subnet NSG instead).

ISSUE: "Private Endpoint created but DNS still returns public IP"
FIX:   Check Private DNS Zone is linked to the VNet.
       Run: az network private-dns link vnet list --zone-name ZONE
       If not linked: az network private-dns link vnet create ...

ISSUE: "VNet Peering shows 'Disconnected'"
FIX:   Peering must be created on BOTH sides.
       One side = "Initiated", both sides = "Connected".

ISSUE: "Storage download fails with 403 even from inside VNet"
FIX:   Check the NSG on pe-subnet allows traffic from your VM's subnet.
       Also: if using Service Endpoint, verify the subnet has the endpoint enabled.

ISSUE: "Route Table not working — traffic still goes direct"
FIX:   Check route table is ATTACHED to the subnet (subnet update --route-table).
       Check IP forwarding is enabled on the NIC AND inside the VM OS.

ISSUE: "az login opens browser but terminal doesn't continue"
FIX:   Use: az login --use-device-code
       This shows a code + URL to visit — no browser redirect needed.

ISSUE: "NAT Gateway data transfer costs unexpected"
FIX:   Remove the NAT Gateway from subnets when not running labs.
       az network vnet subnet update --nat-gateway ""
```
