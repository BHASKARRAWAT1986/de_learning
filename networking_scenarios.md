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

---

## LAB 11 — Azure Portal UI Walkthrough (Labs 1–4 via Click-Through)

> Use this as an alternative to CLI — do the same things visually.
> Great for learning what each resource looks like in the portal.

---

### UI Lab 11A — Create VNet + Subnets via Portal

**Steps:**
```
1. Go to portal.azure.com → search "Virtual networks" → click "+ Create"

2. BASICS tab:
   Subscription:     your subscription
   Resource group:   rg-network-lab  (click "Create new" if doesn't exist)
   Name:             vnet-lab
   Region:           East US

3. Click "Next: IP Addresses"

4. IP ADDRESSES tab:
   IPv4 address space:  10.0.0.0/16   (Azure pre-fills this)
   
   Click "+ Add subnet":
     Name:           subnet-web
     Subnet range:   10.0.1.0/24
     Click "Add"
   
   Click "+ Add subnet" again:
     Name:           subnet-db
     Subnet range:   10.0.2.0/24
     Click "Add"
   
   Click "+ Add subnet" again:
     Name:           subnet-pe
     Subnet range:   10.0.3.0/24
     Click "Add"

5. Click "Review + create" → "Create"

6. After deployment completes (~30 seconds):
   Click "Go to resource"
   Left menu → "Subnets"
   ✅ You should see all 3 subnets listed with their address ranges.

What you'll see in the portal that CLI doesn't show:
  - A visual map of the VNet address space
  - "Available IP addresses: 251" next to each subnet
  - A "Connected devices" count (0 so far)
```

---

### UI Lab 11B — Create NSG + Rules via Portal

**Steps:**
```
1. Search "Network security groups" → "+ Create"
   Resource group: rg-network-lab
   Name:           nsg-web
   Region:         East US
   → "Review + create" → "Create"

2. After creation, click "Go to resource"

3. Left menu → "Inbound security rules" → "+ Add"
   Fill in:
     Source:                  Service Tag
     Source service tag:      Internet
     Source port ranges:      *
     Destination:             Any
     Destination port ranges: 443
     Protocol:                TCP
     Action:                  Allow
     Priority:                100
     Name:                    Allow-HTTPS-Inbound
   → Click "Add"

4. Add another rule for HTTP (port 80):
   Same settings but:
     Destination port ranges: 80
     Priority:                110
     Name:                    Allow-HTTP-Inbound
   → Click "Add"

5. Add SSH rule for your IP:
   Source:                  IP Addresses
   Source IP addresses:     YOUR_PUBLIC_IP (check at https://ifconfig.me)
   Destination port ranges: 22
   Priority:                120
   Name:                    Allow-SSH-MyIP
   → Click "Add"

6. Attach NSG to subnet:
   Left menu → "Subnets" → "+ Associate"
   Virtual network:  vnet-lab
   Subnet:           subnet-web
   → "OK"
   ✅ NSG is now protecting subnet-web

7. Verify rules:
   Left menu → "Inbound security rules"
   You should see your 3 custom rules PLUS 3 Azure default rules.
   Rules are sorted by priority (100, 110, 120, then 65000, 65001, 65500)
```

---

### UI Lab 11C — Create a VM via Portal

**Steps:**
```
1. Search "Virtual machines" → "+ Create" → "Azure virtual machine"

2. BASICS tab:
   Resource group:  rg-network-lab
   VM name:         vm-web
   Region:          East US
   Image:           Ubuntu Server 22.04 LTS  (click "See all images" if not shown)
   Size:            Click "See all sizes" → search "B1s" → Standard_B1s → Select
   
   Authentication type: SSH public key
   Username:            azureuser
   SSH public key:      Generate new key pair (portal will offer to download .pem)
   
   Public inbound ports: None  (we control via NSG)

3. DISKS tab: leave defaults (Standard SSD)

4. NETWORKING tab:  ← MOST IMPORTANT FOR THIS LAB
   Virtual network:    vnet-lab
   Subnet:             subnet-web (10.0.1.0/24)
   Public IP:          (Create new) pip-vm-web, Standard SKU, Static
   NIC network security group: None  (subnet NSG is sufficient)
   
   ✅ You can SEE the VNet and subnet selected here visually

5. MANAGEMENT tab: leave defaults

6. Review + create → Create
   When prompted "Generate new key pair" → "Download private key and create resource"
   → Save the .pem file to your local machine (e.g. C:\Users\you\.ssh\vm-web.pem)

7. After deployment:
   Go to VM → "Connect" → "SSH"
   Portal shows the exact SSH command:
   ssh -i ~/.ssh/vm-web.pem azureuser@20.x.x.x

IMPORTANT NOTE for Windows users:
   The .pem file needs correct permissions:
   Right-click .pem → Properties → Security → Advanced
   → Disable inheritance → Remove all users except yourself
```

---

### UI Lab 11D — Create NAT Gateway via Portal

**Steps:**
```
1. Search "NAT gateways" → "+ Create"

2. BASICS tab:
   Resource group:  rg-network-lab
   Name:            nat-gw-lab
   Region:          East US
   Availability zone: No zone (or zone 1 for resilience)
   Idle timeout:    10 minutes

3. OUTBOUND IP tab:
   Click "Create a new public IP address"
     Name: pip-nat-gateway
     SKU:  Standard
   → OK

4. SUBNET tab:
   Virtual network:  vnet-lab
   Select checkbox:  subnet-web  ✅
   (You can select multiple subnets here — check subnet-db too if you want)

5. Review + create → Create

6. After deployment, go to NAT gateway resource:
   Left menu → "Subnets"
   ✅ You should see subnet-web listed as associated

7. To VERIFY it works:
   Go to vm-web → "Connect" → SSH in
   Run: curl https://ifconfig.me
   Output should be the NAT Gateway's public IP (pip-nat-gateway), not the VM's own IP

Where to find NAT Gateway's public IP:
   Portal → NAT gateway resource → "Outbound IP" → shows the public IP
```

---

### UI Lab 11E — Create Private Endpoint via Portal

**Steps:**
```
1. Go to your Storage Account (created in Lab 6) → Left menu → "Networking"

2. FIREWALLS AND VIRTUAL NETWORKS tab:
   Public network access: Disabled (will set after PE is created)
   First create PE, then disable public.

3. Click "Private endpoint connections" tab → "+ Private endpoint"

4. BASICS tab:
   Resource group:  rg-network-lab
   Name:            pe-storage-blob
   Region:          East US

5. RESOURCE tab:
   Connection method:  Connect to an Azure resource in my directory
   Subscription:       your subscription
   Resource type:      Microsoft.Storage/storageAccounts
   Resource:           your storage account name
   Target sub-resource: blob

6. VIRTUAL NETWORK tab:
   Virtual network:  vnet-lab
   Subnet:           subnet-pe  (10.0.3.0/24)
   
   Private IP configuration: Dynamically allocate IP address
   
   Application security group: (leave blank)

7. DNS tab:
   Integrate with private DNS zone: YES  ← CRITICAL
   Private DNS Zone: (auto-populated) privatelink.blob.core.windows.net
   
   This automatically:
   - Creates the Private DNS Zone
   - Links it to your VNet
   - Adds the A record pointing to the private IP
   
   ✅ Much easier than doing it manually via CLI!

8. Review + create → Create

9. After deployment, verify:
   Go to Storage Account → Networking → Private endpoint connections
   You should see: pe-storage-blob with status "Approved"
   
   Click on the PE resource:
   Left menu → "DNS configuration"
   You should see: labstorage12345.blob.core.windows.net → 10.0.3.4

10. Now disable public access:
    Storage Account → Networking → Firewalls and virtual networks
    Public network access: Disabled → Save
```

---

## LAB 12 — Databricks Workspace + VNet Injection (Classic Compute)

**Goal:** Create a Databricks workspace with VNet injection so clusters run inside YOUR VNet.
**Time:** ~30 minutes
**Cost:** Workspace itself is free; compute charged when clusters run (~$0.10/hour for a small cluster)

### What VNet injection means
```
WITHOUT VNet injection:
  Databricks creates its own managed VNet → your clusters run there
  Your ADLS, SQL, Key Vault are NOT accessible privately → traffic goes via internet

WITH VNet injection:
  You tell Databricks: "put the cluster VMs in MY VNet subnets"
  Clusters get private IPs from YOUR address space
  → Clusters can reach ADLS/SQL via Private Endpoints privately
  → You control NSG rules, routing, firewall
```

### Step 1 — Prepare dedicated subnets for Databricks

**Via CLI:**
```bash
# Create a VNet for this lab (or use existing vnet-lab with new subnets)
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-databricks \
  --address-prefix 10.5.0.0/16 \
  --location eastus

# Subnet 1: Databricks public subnet (cluster driver VMs)
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-public-subnet \
  --address-prefix 10.5.1.0/26

# Subnet 2: Databricks private subnet (cluster worker VMs)
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-private-subnet \
  --address-prefix 10.5.2.0/26

# NOTE: /26 = 64 IPs. Azure uses 5, so 59 usable.
# Each cluster node needs 1 IP. A 10-node cluster needs 11 IPs (1 driver + 10 workers).
# /26 supports ~4-5 clusters running simultaneously.
# Use /24 (256 IPs) for production.
```

**Via Portal:**
```
1. Search "Virtual networks" → "+ Create"
   Name: vnet-databricks
   Address space: 10.5.0.0/16
   Region: East US

2. IP Addresses tab → "+ Add subnet":
   Name: dbr-public-subnet
   Range: 10.5.1.0/26

3. "+ Add subnet" again:
   Name: dbr-private-subnet
   Range: 10.5.2.0/26

4. Review + create → Create
```

### Step 2 — Create NSG for Databricks subnets (required rules)
```bash
# Create NSG
az network nsg create \
  --resource-group rg-network-lab \
  --name nsg-databricks

# REQUIRED rule 1: Allow traffic within the VNet (cluster ↔ cluster Spark shuffle)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-databricks \
  --name Allow-VNet-Internal \
  --priority 100 \
  --direction Inbound \
  --access Allow \
  --protocol "*" \
  --source-address-prefix VirtualNetwork \
  --source-port-range "*" \
  --destination-address-prefix VirtualNetwork \
  --destination-port-range "*"

# REQUIRED rule 2: Allow Databricks control plane to reach cluster VMs
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-databricks \
  --name Allow-Databricks-ControlPlane \
  --priority 110 \
  --direction Inbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix AzureDatabricks \
  --source-port-range "*" \
  --destination-address-prefix VirtualNetwork \
  --destination-port-range "22 9001"

# REQUIRED outbound rule: Clusters reach Databricks control plane
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-databricks \
  --name Allow-Outbound-Databricks \
  --priority 100 \
  --direction Outbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix VirtualNetwork \
  --source-port-range "*" \
  --destination-address-prefix AzureDatabricks \
  --destination-port-range 443

# REQUIRED outbound rule: Clusters reach Azure Storage (download init scripts, DBFS)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-databricks \
  --name Allow-Outbound-Storage \
  --priority 110 \
  --direction Outbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix VirtualNetwork \
  --source-port-range "*" \
  --destination-address-prefix AzureStorage \
  --destination-port-range 443

# Attach NSG to BOTH Databricks subnets
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-public-subnet \
  --network-security-group nsg-databricks

az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-private-subnet \
  --network-security-group nsg-databricks
```

### Step 3 — Delegate subnets to Databricks
```bash
# Both subnets must be delegated to Microsoft.Databricks/workspaces
# This reserves them exclusively for Databricks

az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-public-subnet \
  --delegations Microsoft.Databricks/workspaces

az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-private-subnet \
  --delegations Microsoft.Databricks/workspaces

# Verify delegation:
az network vnet subnet show \
  --resource-group rg-network-lab \
  --vnet-name vnet-databricks \
  --name dbr-public-subnet \
  --query delegations[0].serviceName -o tsv

# Expected: Microsoft.Databricks/workspaces
```

**Via Portal:**
```
1. Go to vnet-databricks → Subnets → click "dbr-public-subnet"

2. Under "Subnet delegation":
   Delegate subnet to a service: Microsoft.Databricks/workspaces
   → Save

3. Repeat for dbr-private-subnet
```

### Step 4 — Create the Databricks Workspace with VNet Injection

**Via Portal (recommended for VNet injection — easier):**
```
1. Search "Azure Databricks" → "+ Create"

2. BASICS tab:
   Resource group:  rg-network-lab
   Workspace name:  dbr-workspace-lab
   Region:          East US
   Pricing tier:    Premium  (required for Unity Catalog, Private Link)
                    (Standard is fine for just VNet injection testing)

3. NETWORKING tab:  ← KEY TAB
   Deploy Azure Databricks workspace in your own Virtual Network (VNet):
   Toggle: YES  ← turn this ON
   
   Virtual Network: vnet-databricks
   Public subnet name:  dbr-public-subnet
   Public subnet CIDR:  10.5.1.0/26   (auto-filled)
   Private subnet name: dbr-private-subnet
   Private subnet CIDR: 10.5.2.0/26   (auto-filled)
   
   No Public IP (Secure cluster connectivity): 
     Enable this for production (clusters have no public IPs)
     For lab: can leave disabled (easier to manage)

4. Review + create → Create
   Deployment takes ~5 minutes.

What gets created:
   - Databricks workspace resource (in YOUR resource group)
   - A "managed resource group" (auto-created by Azure):
     Name: databricks-rg-dbr-workspace-lab-XXXXXXX
     Contains: DBFS storage account, managed disks
     You can SEE this but shouldn't modify it.
```

**Via CLI:**
```bash
az databricks workspace create \
  --resource-group rg-network-lab \
  --name dbr-workspace-lab \
  --location eastus \
  --sku standard \
  --custom-virtual-network-id $(az network vnet show -g rg-network-lab -n vnet-databricks --query id -o tsv) \
  --custom-public-subnet-name dbr-public-subnet \
  --custom-private-subnet-name dbr-private-subnet \
  --no-public-ip false

# Takes ~5 minutes
# Expected: "provisioningState": "Succeeded"
```

### Step 5 — Verify VNet injection worked
```
Portal steps:
1. Go to dbr-workspace-lab resource → "Launch Workspace"
2. Create a cluster: Compute → "+ Create compute"
   Set:
     Cluster name: test-cluster
     Single node (for lab — cheapest)
     Runtime: 14.x LTS
   → Create

3. While cluster is STARTING (takes 5-7 min):
   Go to Azure Portal → Resource Group → rg-network-lab
   
   You should see NEW resources appearing:
   - Virtual machines (names like: worker-xxxx) in the vnet-databricks
   - Network interfaces (NICs) with IPs from 10.5.1.x and 10.5.2.x
   
   ✅ This proves the cluster VMs are in YOUR VNet subnets!

4. From a notebook:
   Create a notebook → attach to test-cluster
   Run: %sh ip addr
   
   Expected output shows IP like: 10.5.2.x (from your private subnet)
   ✅ Cluster VM has a private IP from YOUR address space

5. IMPORTANT: Terminate the cluster when done to stop charges
   Compute → three dots menu → Terminate
```

---

## LAB 13 — Databricks Job Compute Networking (Classic Clusters)

**Goal:** Configure a job cluster to read from ADLS Gen2 using Private Endpoint, access Key Vault for secrets, and understand the network path.
**Time:** ~30 minutes
**Cost:** Cluster compute ~$0.15 for this lab

### Step 1 — Create ADLS and Key Vault with Private Endpoints

**Create ADLS Gen2:**
```bash
# Create ADLS (must enable hierarchical namespace for Gen2)
ADLS_NAME="adlslabdatabricks$(date +%s | tail -c 5)"
echo "ADLS name: $ADLS_NAME"

az storage account create \
  --resource-group rg-network-lab \
  --name $ADLS_NAME \
  --sku Standard_LRS \
  --kind StorageV2 \
  --location eastus \
  --enable-hierarchical-namespace true   # ← This makes it ADLS Gen2 (not just Blob)

# Create a container (called "filesystem" in ADLS)
az storage fs create \
  --name bronze \
  --account-name $ADLS_NAME \
  --auth-mode login

# Upload a sample parquet-like file for testing
echo "col1,col2,col3" > sample.csv
echo "1,hello,2024" >> sample.csv
echo "2,world,2024" >> sample.csv

az storage fs file upload \
  --source sample.csv \
  --path data/sample.csv \
  --file-system bronze \
  --account-name $ADLS_NAME \
  --auth-mode login
```

**Create Private Endpoint for ADLS DFS endpoint:**
```bash
ADLS_ID=$(az storage account show -g rg-network-lab -n $ADLS_NAME --query id -o tsv)

# Create PE for DFS endpoint (ABFSS protocol used by Spark/Databricks)
az network private-endpoint create \
  --resource-group rg-network-lab \
  --name pe-adls-dfs \
  --vnet-name vnet-databricks \
  --subnet dbr-private-subnet \
  --private-connection-resource-id $ADLS_ID \
  --group-id dfs \
  --connection-name conn-adls-dfs

# Get the assigned private IP
az network private-endpoint show \
  --resource-group rg-network-lab \
  --name pe-adls-dfs \
  --query customDnsConfigs \
  --output table

# Expected:
# Fqdn                                              IpAddresses
# ------------------------------------------------  -----------
# adlslabdatabricks12345.dfs.core.windows.net       10.5.2.5
```

**Create Private DNS Zone for ADLS DFS:**
```bash
az network private-dns zone create \
  --resource-group rg-network-lab \
  --name "privatelink.dfs.core.windows.net"

az network private-dns link vnet create \
  --resource-group rg-network-lab \
  --zone-name "privatelink.dfs.core.windows.net" \
  --name dns-link-databricks-vnet \
  --virtual-network vnet-databricks \
  --registration-enabled false

az network private-dns record-set a add-record \
  --resource-group rg-network-lab \
  --zone-name "privatelink.dfs.core.windows.net" \
  --record-set-name $ADLS_NAME \
  --ipv4-address 10.5.2.5

# Disable public access on ADLS
az storage account update \
  --resource-group rg-network-lab \
  --name $ADLS_NAME \
  --public-network-access Disabled
```

### Step 2 — Create a Service Principal for Databricks to access ADLS

```bash
# Create a Service Principal
SP=$(az ad sp create-for-rbac \
  --name "sp-databricks-lab" \
  --skip-assignment \
  --output json)

SP_APP_ID=$(echo $SP | python3 -c "import sys,json; print(json.load(sys.stdin)['appId'])")
SP_SECRET=$(echo $SP | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
TENANT_ID=$(az account show --query tenantId -o tsv)

echo "App ID: $SP_APP_ID"
echo "Secret: $SP_SECRET"
echo "Tenant: $TENANT_ID"
# SAVE THESE — you'll need them in Databricks

# Grant the SP access to ADLS
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $SP_APP_ID \
  --scope $(az storage account show -g rg-network-lab -n $ADLS_NAME --query id -o tsv)

# Expected: role assignment JSON with "roleDefinitionName": "Storage Blob Data Contributor"
```

**Via Portal:**
```
1. Go to Storage Account → "Access Control (IAM)"
2. "+ Add" → "Add role assignment"
3. Role: "Storage Blob Data Contributor"
4. Members: "+ Select members" → search for "sp-databricks-lab" → Select
5. Review + assign
```

### Step 3 — Create a cluster and test ADLS access from a notebook

**In Databricks UI:**
```
1. Go to Databricks workspace → Compute → "+ Create compute"
   
   Name: test-job-cluster
   Single node
   Access mode: Single user (your email)
   Spark config (Advanced options):
     Add these spark configs:
     fs.azure.account.auth.type.<ADLS_NAME>.dfs.core.windows.net    OAuth
     fs.azure.account.oauth.provider.type.<ADLS_NAME>.dfs.core.windows.net    org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
     fs.azure.account.oauth2.client.id.<ADLS_NAME>.dfs.core.windows.net    <SP_APP_ID>
     fs.azure.account.oauth2.client.secret.<ADLS_NAME>.dfs.core.windows.net    <SP_SECRET>
     fs.azure.account.oauth2.client.endpoint.<ADLS_NAME>.dfs.core.windows.net    https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
   
   → Create

2. Wait for cluster to start (~5 min)

3. Create a new notebook → attach to test-job-cluster

4. In a cell, run:
   # Test 1: DNS resolves to PRIVATE IP (not public)
   %sh nslookup <ADLS_NAME>.dfs.core.windows.net
   # Expected: Address: 10.5.2.5  (private endpoint IP)
   
   # Test 2: Read the CSV file from ADLS
   df = spark.read.csv(
     "abfss://bronze@<ADLS_NAME>.dfs.core.windows.net/data/sample.csv",
     header=True
   )
   df.show()
   # Expected:
   # +----+-----+----+
   # |col1| col2|col3|
   # +----+-----+----+
   # |   1|hello|2024|
   # |   2|world|2024|
   # +----+-----+----+
   
   # Test 3: Verify traffic went through private endpoint (not internet)
   %sh traceroute <ADLS_NAME>.dfs.core.windows.net
   # Expected: first hop should be within 10.5.x.x range (your private subnet)
   #           NOT going to 20.150.x.x (public Azure IP)

5. TERMINATE CLUSTER WHEN DONE (Compute → Terminate)
```

### Step 4 — Network flow diagram for what just happened
```
Notebook cell runs: spark.read.csv("abfss://bronze@adls.dfs.core.windows.net/...")
                                    │
                     ┌──────────────▼──────────────────────────────────┐
                     │  Cluster Driver VM (10.5.2.x) in dbr-private-subnet │
                     │                                                   │
                     │  1. DNS: adlslabdatabricks.dfs.core.windows.net   │
                     │     → 168.63.129.16 (Azure DNS)                   │
                     │     → Private DNS Zone → 10.5.2.5  ✅             │
                     │                                                   │
                     │  2. Gets OAuth token from Azure AD for SP         │
                     │     (outbound to login.microsoftonline.com:443)   │
                     │     → via NSG Allow-Outbound-Storage rule         │
                     │                                                   │
                     │  3. Sends HTTP GET to 10.5.2.5:443               │
                     │     (Private Endpoint NIC in same subnet)         │
                     │                                                   │
                     │  4. ADLS validates token → returns data           │
                     └──────────────────────────────────────────────────┘
                                           │
                     Private Endpoint (10.5.2.5) → ADLS Gen2
                     ✅ Traffic NEVER left vnet-databricks
```

---

## LAB 14 — Databricks SQL Warehouse (Serverless) Networking

**Goal:** Create a SQL Warehouse, configure Network Connectivity Configuration (NCC) so the serverless compute can reach ADLS via Private Endpoint without going through public internet.
**Time:** ~25 minutes
**Cost:** SQL Warehouse ~$0.22/DBU while running; NCC PE ~$0.01/hour

### Understanding Serverless SQL Warehouse networking
```
The PROBLEM:
  SQL Warehouse (serverless) runs in DATABRICKS'S Azure subscription/VNet — not yours.
  Your ADLS is in YOUR Azure subscription.
  
  How does serverless compute reach YOUR private ADLS?

TWO OPTIONS:
  Option A (simple, less secure):
    Add Databricks stable outbound IPs to your ADLS storage firewall allowlist.
    Traffic goes through Databricks' managed NAT → your ADLS firewall allows those IPs.
    Not truly private — traffic goes through Databricks' managed network.

  Option B (NCC — secure):
    Create a Private Endpoint in YOUR VNet.
    Create a Network Connectivity Configuration (NCC) in Databricks.
    NCC says: "route traffic to ADLS through this Private Endpoint."
    → All SQL Warehouse traffic to ADLS goes through YOUR VNet's PE.
    → ADLS public access can be fully disabled. ✅
```

### Step 1 — Create a SQL Warehouse (Serverless)

**Via Databricks UI:**
```
1. Go to Databricks workspace → SQL Warehouses (left sidebar) → "+ Create"

   Name:     lab-sql-warehouse
   Cluster size:  2X-Small  (cheapest for learning)
   Auto-stop: 10 minutes  ← important for cost control
   Type:  Serverless  ← SELECT THIS (not Classic, not Pro)
   
   → Create

2. Wait for it to start (30-60 seconds — much faster than classic cluster!)

3. The warehouse is RUNNING but can it access your private ADLS?
   Let's test — in "SQL Editor" (left sidebar):
   
   -- Create an external location pointing to your ADLS
   -- (This will likely FAIL if ADLS has public access disabled)
   SELECT * FROM read_files(
     'abfss://bronze@<ADLS_NAME>.dfs.core.windows.net/data/sample.csv',
     format => 'csv',
     header => true
   )
   
   If ADLS public access is disabled:
   → Error: "Connection timed out" or "403 Forbidden"
   ✅ This confirms the problem — serverless can't reach private ADLS yet
```

### Step 2 — Option A: Stable Outbound IPs (quick but less secure)

**Find Databricks serverless outbound IPs:**
```
1. Go to Databricks Account Console (accounts.azuredatabricks.net)
   (This is different from the workspace — it's the account level)

2. Settings → IP Access List / Network Policies
   OR: Go to your SQL Warehouse → Edit → Advanced options
   → Shows the outbound IP ranges for your region

   Alternatively, Databricks publishes these at:
   https://learn.microsoft.com/en-us/azure/databricks/resources/supported-regions
   → Find your region → note the "Serverless outbound IPs"
   
   Example for East US: 20.49.x.x/28, 20.119.x.x/28 (check current docs)
```

**Add those IPs to ADLS storage firewall:**
```bash
# Re-enable public access with IP restriction (not fully disabled)
az storage account update \
  --resource-group rg-network-lab \
  --name $ADLS_NAME \
  --public-network-access Enabled \
  --default-action Deny

# Add Databricks serverless outbound IPs (replace with actual IPs from docs)
az storage account network-rule add \
  --resource-group rg-network-lab \
  --account-name $ADLS_NAME \
  --ip-address "20.49.x.x/28"   # Replace with actual Databricks serverless IP for East US

# Now test again in SQL Warehouse SQL Editor:
# SELECT * FROM read_files('abfss://bronze@<ADLS_NAME>.dfs.core.windows.net/...')
# Should work now (traffic goes via Databricks NAT → your ADLS firewall allows it)
```

### Step 3 — Option B: NCC with Private Endpoint (secure)

**Step 3a — Create a Private Endpoint for ADLS DFS (in a new subnet):**
```bash
# Add a new subnet to an existing VNet for serverless PE
# We'll use vnet-lab from Lab 1
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-lab \
  --name subnet-serverless-pe \
  --address-prefix 10.0.5.0/24

# Create PE for ADLS DFS in this subnet
az network private-endpoint create \
  --resource-group rg-network-lab \
  --name pe-adls-serverless \
  --vnet-name vnet-lab \
  --subnet subnet-serverless-pe \
  --private-connection-resource-id $ADLS_ID \
  --group-id dfs \
  --connection-name conn-adls-serverless

# Note the private IP assigned
az network private-endpoint show \
  --resource-group rg-network-lab \
  --name pe-adls-serverless \
  --query customDnsConfigs \
  --output table
# Expected: 10.0.5.4
```

**Step 3b — Create NCC in Databricks Account Console:**
```
IMPORTANT: NCC is configured in the ACCOUNT console, not the workspace.

1. Go to: accounts.azuredatabricks.net
   (Use your Azure AD credentials)

2. Left menu → "Network" → "Network connectivity"

3. Click "+ Create network connectivity configuration"
   Name:     ncc-prod-eastus
   Region:   East US  ← must match your workspace region

4. Click "Create"
```

**Step 3c — Add Private Endpoint rule to NCC:**
```
In the NCC you just created:

1. Click "+ Add private endpoint rule"

2. Fill in:
   Resource type:      Microsoft.Storage/storageAccounts
   Resource ID:        (paste the full resource ID of $ADLS_NAME storage account)
                       /subscriptions/.../storageAccounts/<ADLS_NAME>
   Sub-resource:       dfs  ← for ADLS Gen2 (use 'blob' for blob storage)
   Group ID:           dfs

3. Click "Add"

4. Status will show: "Pending approval"
   (Databricks has sent a Private Endpoint Connection Request to your ADLS)
```

**Step 3d — Approve the Private Endpoint connection in Azure:**
```
1. Go to Azure Portal → Storage Account ($ADLS_NAME)
2. Left menu → "Networking" → "Private endpoint connections" tab
3. You should see a new connection from Databricks with status "Pending"
   Name: something like "ncc-prod-eastus-xxxxxx"
4. Select it → "Approve"
   Reason (optional): "Databricks NCC serverless access"
5. Status changes to: "Approved" ✅

Back in Databricks Account Console:
  NCC → Private endpoint rules tab
  Status should change from "Pending" to "Active" (may take a few minutes)
```

**Step 3e — Attach NCC to your workspace:**
```
1. In Databricks Account Console → Workspaces
2. Click on "dbr-workspace-lab"
3. "Network" tab (or similar)
4. Network connectivity configuration: select "ncc-prod-eastus"
5. Save

Alternative via Databricks Workspace Admin:
  Workspace → Admin Settings → Networking → Serverless compute
  → Set Network Connectivity Configuration: ncc-prod-eastus
```

**Step 3f — Disable public access on ADLS (now safe):**
```bash
az storage account update \
  --resource-group rg-network-lab \
  --name $ADLS_NAME \
  --public-network-access Disabled
```

**Step 3g — Test in SQL Warehouse:**
```sql
-- In Databricks SQL Editor (SQL Warehouse running)
-- This should now work even with ADLS public access disabled

SELECT * FROM read_files(
  'abfss://bronze@<ADLS_NAME>.dfs.core.windows.net/data/sample.csv',
  format => 'csv',
  header => true
)

-- Expected output:
-- col1 | col2  | col3
-- 1    | hello | 2024
-- 2    | world | 2024

-- ✅ Serverless SQL Warehouse reading from private ADLS via NCC+PE
```

### Network flow with NCC
```
SQL Warehouse query runs:
  SELECT * FROM read_files('abfss://bronze@adls.dfs.core.windows.net/...')
                │
  ┌─────────────▼──────────────────────────────────────────────────────┐
  │ Databricks Serverless VNet (Databricks' Azure subscription)         │
  │                                                                      │
  │  Serverless compute node                                             │
  │  Checks NCC: "is there a PE rule for adls.dfs.core.windows.net?"   │
  │  YES → route through Private Endpoint: pe-adls-serverless           │
  └──────────────────────────────┬──────────────────────────────────────┘
                                 │ Private backbone connection
                    ┌────────────▼────────────────────────────────────┐
                    │ YOUR VNet: vnet-lab                               │
                    │   subnet-serverless-pe (10.0.5.0/24)             │
                    │   pe-adls-serverless NIC (10.0.5.4)              │
                    │        │                                          │
                    │        │ (Azure private backbone)                 │
                    │        ▼                                          │
                    │   ADLS Gen2: $ADLS_NAME                           │
                    │   (Public access: DISABLED)                       │
                    │   Only PE connection from NCC is accepted ✅      │
                    └─────────────────────────────────────────────────┘
```

---

## LAB 15 — Databricks Serverless Jobs Networking

**Goal:** Understand how serverless job compute differs from SQL Warehouse, and configure NCC for both.
**Time:** ~15 minutes (conceptual + config)

### Serverless Jobs vs SQL Warehouse — networking difference
```
┌─────────────────────────────┬──────────────────────────┬────────────────────────────┐
│ Aspect                      │ SQL Warehouse (Serverless)│ Serverless Jobs            │
├─────────────────────────────┼──────────────────────────┼────────────────────────────┤
│ What runs on it?            │ SQL queries only          │ Python/Scala/SQL notebooks │
│                             │ (ANSI SQL, Delta Lake)    │ and job tasks              │
│ NCC supported?              │ ✅ Yes                    │ ✅ Yes (same NCC)          │
│ VNet injection possible?    │ ❌ No (serverless)        │ ❌ No (serverless)         │
│ Startup time                │ 30–60 seconds             │ 30–90 seconds              │
│ Private data access         │ NCC + Private Endpoint    │ NCC + Private Endpoint     │
│ Access on-prem              │ Private Network Gateway   │ Private Network Gateway    │
│ Cost model                  │ DBU per second used       │ DBU per second used        │
└─────────────────────────────┴──────────────────────────┴────────────────────────────┘
```

### Step 1 — Create a Serverless Job

**Via Databricks UI:**
```
1. Go to Workflows (left sidebar) → "+ Create job"

2. Task:
   Task name:   ingest-bronze
   Type:        Notebook
   Source:      Workspace
   Path:        (create a notebook first — see below)
   Compute:     Serverless  ← SELECT THIS

3. Create the notebook first:
   Workspace → Create → Notebook
   Name: ingest_bronze
   Content:
   
   # Ingest data from ADLS
   ADLS_NAME = "<your_adls_name>"
   
   df = spark.read.csv(
     f"abfss://bronze@{ADLS_NAME}.dfs.core.windows.net/data/sample.csv",
     header=True,
     inferSchema=True
   )
   df.show()
   print(f"Rows read: {df.count()}")
   
   # Write back as Delta (to prove write works too)
   df.write.format("delta").mode("overwrite").save(
     f"abfss://bronze@{ADLS_NAME}.dfs.core.windows.net/delta/sample"
   )
   print("Write complete")

4. Back in job creation:
   Select the notebook path
   Compute: Serverless
   → Create

5. Click "Run now"
   The job uses the SAME NCC you configured for the SQL Warehouse.
   (NCC is workspace-level, applies to all serverless compute in that workspace)
```

### Step 2 — Verify networking in Job run logs
```
After job completes:
1. Click on the run → "Output"
2. Click "Cluster" tab in the run details
3. Look for: "Using serverless compute"
4. Check "Spark UI" → "Environment" tab
   → Shows ADLS credentials and config
   
5. In the notebook output:
   If it shows row count and "Write complete" → ✅ NCC is working
   If it shows 403/timeout → NCC PE not approved yet or NCC not attached to workspace

Job run logs vs cluster logs:
  Serverless logs go to: Run details → Driver logs
  There's no long-lived cluster to check (cluster is ephemeral — spins up per run)
```

---

## LAB 16 — Databricks Private Network Gateway (On-Premises to Serverless)

**Goal:** Connect Databricks serverless compute to on-premises resources (databases, APIs) using Databricks Private Network Gateway — so serverless jobs can read from on-prem Oracle/SQL without exposing them to internet.
**Time:** ~45 minutes
**Cost:** Private Network Gateway ~ $0.10/hour for the gateway resource + VPN Gateway costs

### What is Databricks Private Network Gateway?
```
PROBLEM:
  Classic clusters (VNet injected) can reach on-prem via VPN/ExpressRoute easily.
  (Cluster VMs are in YOUR VNet → VPN tunnel → on-prem)
  
  Serverless compute runs in DATABRICKS's VNet → has NO access to your VPN tunnel.
  
  So how can a serverless notebook/job read from on-prem Oracle DB?
  Previously: only option was classic clusters.
  NOW: Databricks Private Network Gateway solves this.

HOW IT WORKS:
  1. You deploy a "Private Network Gateway" VM in YOUR VNet
     (a small Azure VM running the Databricks gateway agent)
  2. This gateway VM connects to:
     a. Your on-prem resources (via VPN/ExpressRoute that's already set up)
     b. Databricks serverless compute (via a private relay connection)
  3. Databricks serverless → relay → gateway VM → on-prem resource
  4. Response comes back the same way

VISUAL:
  Serverless compute ←──── private relay ────→ Gateway VM (your VNet)
                                                    │
                                               VPN/ExpressRoute
                                                    │
                                              On-Prem Oracle/SQL Server
```

### Step 1 — Prerequisites for this lab
```
Before this lab you need:
✅ Databricks workspace (Premium tier — required for Private Network Gateway)
✅ A VNet with on-prem connectivity (VPN Gateway or ExpressRoute)
   For this lab: we simulate on-prem with a VM in a separate VNet + peering

For the lab simulation:
  "On-prem" = another VNet/VM with a simulated SQL server (PostgreSQL)
  "VPN" = VNet peering (simulates the tunnel in a lab environment)
```

### Step 2 — Set up the simulated "on-prem" environment
```bash
# Create "on-prem" VNet (simulating your datacenter network)
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-onprem \
  --address-prefix 192.168.0.0/24 \
  --location eastus

az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-onprem \
  --name subnet-onprem \
  --address-prefix 192.168.0.0/27

# Create "on-prem" VM with PostgreSQL (simulates on-prem Oracle/SQL)
az vm create \
  --resource-group rg-network-lab \
  --name vm-onprem-db \
  --image Ubuntu2204 \
  --size Standard_B1s \
  --vnet-name vnet-onprem \
  --subnet subnet-onprem \
  --public-ip-address "" \
  --nsg "" \
  --admin-username azureuser \
  --generate-ssh-keys

# Install PostgreSQL on the "on-prem" VM (from Cloud Shell using jump host)
# First, add a temp public IP to access it for setup
az network public-ip create -g rg-network-lab --name pip-onprem-temp --sku Standard
NIC_ONPREM=$(az vm show -g rg-network-lab -n vm-onprem-db --query networkProfile.networkInterfaces[0].id -o tsv | sed 's|.*/||')
az network nic ip-config update -g rg-network-lab --nic-name $NIC_ONPREM --name ipconfig1 --public-ip-address pip-onprem-temp

# SSH into the on-prem VM
ONPREM_IP=$(az network public-ip show -g rg-network-lab --name pip-onprem-temp --query ipAddress -o tsv)
ssh azureuser@$ONPREM_IP

# Inside vm-onprem-db: install and configure PostgreSQL
sudo apt-get update -y
sudo apt-get install -y postgresql postgresql-contrib

# Start PostgreSQL and create test DB
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create test database and table
sudo -u postgres psql <<EOF
CREATE USER labuser WITH PASSWORD 'LabP@ss2024';
CREATE DATABASE labdb;
GRANT ALL PRIVILEGES ON DATABASE labdb TO labuser;
\c labdb
CREATE TABLE sales (id INT, product VARCHAR(50), amount DECIMAL);
INSERT INTO sales VALUES (1, 'Widget A', 100.00);
INSERT INTO sales VALUES (2, 'Widget B', 250.00);
INSERT INTO sales VALUES (3, 'Widget C', 75.50);
GRANT ALL ON TABLE sales TO labuser;
EOF

# Configure PostgreSQL to allow remote connections
echo "host all all 192.168.0.0/24 md5" | sudo tee -a /etc/postgresql/14/main/pg_hba.conf
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/14/main/postgresql.conf
sudo systemctl restart postgresql

exit
# Remove the temp public IP after setup
az network nic ip-config update -g rg-network-lab --nic-name $NIC_ONPREM --name ipconfig1 --remove publicIpAddress
az network public-ip delete -g rg-network-lab --name pip-onprem-temp
```

### Step 3 — Set up the Gateway VNet (your corporate Azure VNet)
```bash
# Create a dedicated VNet for the Private Network Gateway
az network vnet create \
  --resource-group rg-network-lab \
  --name vnet-gateway \
  --address-prefix 10.10.0.0/16 \
  --location eastus

# Subnet for the gateway VM
az network vnet subnet create \
  --resource-group rg-network-lab \
  --vnet-name vnet-gateway \
  --name subnet-gateway \
  --address-prefix 10.10.1.0/24

# Peer vnet-gateway with vnet-onprem (simulates VPN to on-prem)
GW_VNET_ID=$(az network vnet show -g rg-network-lab -n vnet-gateway --query id -o tsv)
ONPREM_VNET_ID=$(az network vnet show -g rg-network-lab -n vnet-onprem --query id -o tsv)

az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-gw-to-onprem \
  --vnet-name vnet-gateway \
  --remote-vnet $ONPREM_VNET_ID \
  --allow-vnet-access

az network vnet peering create \
  --resource-group rg-network-lab \
  --name peer-onprem-to-gw \
  --vnet-name vnet-onprem \
  --remote-vnet $GW_VNET_ID \
  --allow-vnet-access

# Verify: vnet-gateway can reach vnet-onprem (192.168.0.0/24) ✅
```

### Step 4 — Deploy the Databricks Private Network Gateway

**Via Databricks UI (Account Console — Premium required):**
```
1. Go to: accounts.azuredatabricks.net
   Left menu → "Network" → "Private network gateways"  (or "Network access")

2. Click "+ Add" / "+ Create gateway"

3. Fill in:
   Gateway name:     pngw-lab-eastus
   Region:           East US
   
   Network details:
   Subscription:     your Azure subscription
   Resource group:   rg-network-lab
   Virtual network:  vnet-gateway
   Subnet:           subnet-gateway

4. Click "Create"

WHAT DATABRICKS DOES BEHIND THE SCENES:
  - Creates an Azure VM (the gateway agent) in your subnet-gateway
    VM name: something like "databricks-pngw-xxxxxxxx"
    This VM's IP: 10.10.1.4 (from subnet-gateway)
  - The gateway agent establishes a SECURE OUTBOUND connection to
    Databricks' relay service (HTTPS/443 — outbound only from YOUR side)
  - No inbound rules needed — YOUR gateway calls OUT to Databricks
  - Databricks relay then lets serverless compute send traffic THROUGH
    this established connection back to your on-prem resources

5. After creation, status should show: "Running" or "Active"
   (Takes 3–5 minutes for the VM to boot and agent to connect)
```

**Verify the gateway VM was created:**
```bash
# Check the VM created by Databricks in your subnet
az vm list \
  --resource-group rg-network-lab \
  --output table \
  --query "[?contains(name,'databricks')]"

# Expected: a VM named something like "databricks-pngw-xxxx" in subnet-gateway
# Private IP: 10.10.1.4

# Check it can reach the on-prem PostgreSQL
az vm run-command invoke \
  --resource-group rg-network-lab \
  --name databricks-pngw-xxxx \
  --command-id RunShellScript \
  --scripts "nc -zv 192.168.0.4 5432 && echo 'PostgreSQL reachable' || echo 'Cannot reach PostgreSQL'"

# Expected: "PostgreSQL reachable"
# ✅ Gateway VM can reach on-prem DB via the VNet peering (simulating VPN)
```

### Step 5 — Configure NSG for the gateway subnet
```bash
az network nsg create \
  --resource-group rg-network-lab \
  --name nsg-gateway

# Allow outbound HTTPS to Databricks relay (gateway agent calls out)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-gateway \
  --name Allow-Outbound-Databricks-Relay \
  --priority 100 \
  --direction Outbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix VirtualNetwork \
  --source-port-range "*" \
  --destination-address-prefix AzureDatabricks \
  --destination-port-range 443

# Allow outbound to on-prem (via peering/VPN)
az network nsg rule create \
  --resource-group rg-network-lab \
  --nsg-name nsg-gateway \
  --name Allow-Outbound-OnPrem \
  --priority 110 \
  --direction Outbound \
  --access Allow \
  --protocol Tcp \
  --source-address-prefix 10.10.1.0/24 \
  --source-port-range "*" \
  --destination-address-prefix 192.168.0.0/24 \
  --destination-port-range "*"

# Attach NSG
az network vnet subnet update \
  --resource-group rg-network-lab \
  --vnet-name vnet-gateway \
  --name subnet-gateway \
  --network-security-group nsg-gateway
```

### Step 6 — Attach the Private Network Gateway to a Workspace

**Via Databricks Account Console:**
```
1. Account Console → Workspaces → dbr-workspace-lab

2. "Network" tab → "Private network gateway"
   Select:  pngw-lab-eastus
   → Save

OR via Workspace Admin Settings:
1. Workspace → Admin Settings (top right user menu)
2. "Networking" section
3. "Private network gateway": select pngw-lab-eastus
4. Save
```

### Step 7 — Install PostgreSQL JDBC driver on Databricks and read on-prem data

**Create an init script to install the JDBC driver:**
```python
# In a Databricks notebook (classic cluster or via dbutils):

# Upload JDBC driver init script to DBFS
init_script = """#!/bin/bash
# Install PostgreSQL JDBC driver
wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
     -O /databricks/jars/postgresql-42.7.1.jar
"""

dbutils.fs.put("/databricks/init_scripts/install_pg_jdbc.sh", init_script, overwrite=True)
print("Init script uploaded")
```

**Create a serverless-capable cluster with the init script (for job compute):**
```
In Databricks UI:
  Compute → Create compute
  
  Name: job-cluster-onprem
  
  Advanced options → Init scripts:
    Workspace: /databricks/init_scripts/install_pg_jdbc.sh
  
  Spark config:
    spark.jars /databricks/jars/postgresql-42.7.1.jar
  
  Create and wait for startup
```

**Read from on-prem PostgreSQL:**
```python
# In a notebook attached to job-cluster-onprem (or as a serverless job task)

# Connection details for "on-prem" PostgreSQL
jdbc_url = "jdbc:postgresql://192.168.0.4:5432/labdb"
# 192.168.0.4 = the on-prem VM's private IP
# Traffic path:
#   Notebook → Private Network Gateway → peering → on-prem VM:5432

connection_properties = {
    "user": "labuser",
    "password": "LabP@ss2024",
    "driver": "org.postgresql.Driver"
}

# Read the sales table
df = spark.read.jdbc(
    url=jdbc_url,
    table="sales",
    properties=connection_properties
)

df.show()
# Expected:
# +---+---------+------+
# | id|  product|amount|
# +---+---------+------+
# |  1| Widget A|100.00|
# |  2| Widget B|250.00|
# |  3| Widget C| 75.50|
# +---+---------+------+

print("✅ Successfully read from on-prem PostgreSQL via Databricks Private Network Gateway!")

# Write results back to ADLS as Delta (via NCC + PE)
df.write.format("delta").mode("overwrite").save(
    f"abfss://bronze@{ADLS_NAME}.dfs.core.windows.net/delta/sales_from_onprem"
)
print("✅ Written to ADLS via Private Endpoint!")
```

### Step 8 — Full traffic flow diagram
```
Serverless Notebook runs:
  spark.read.jdbc("jdbc:postgresql://192.168.0.4:5432/labdb", ...)
                         │
  ┌────────────────────  │  ────────────────────────────────────────────┐
  │ DATABRICKS serverless │ VNet                                          │
  │                       │                                               │
  │  Serverless node      │                                               │
  │  checks: does this    │                                               │
  │  workspace have a     │                                               │
  │  Private Network GW?  │                                               │
  │  YES → pngw-lab       │                                               │
  │                       │                                               │
  │  Routes 192.168.x.x   │                                               │
  │  traffic through      │                                               │
  │  secure relay to GW   │                                               │
  └──────────────────── │ ────────────────────────────────────────────┘
                        │
                   (Databricks private relay — HTTPS 443)
                        │
  ┌─────────────────────▼──────────────────────────────────────────────┐
  │ YOUR vnet-gateway (10.10.0.0/16)                                     │
  │                                                                       │
  │  Gateway VM: databricks-pngw-xxxx (10.10.1.4)                        │
  │  → receives request: connect to 192.168.0.4:5432                     │
  │  → forwards to on-prem via VNet peering                              │
  └──────────────────────────────┬──────────────────────────────────────┘
                                 │ VNet peering (simulates VPN/ExpressRoute)
  ┌──────────────────────────────▼──────────────────────────────────────┐
  │ vnet-onprem (192.168.0.0/24)                                         │
  │                                                                       │
  │  vm-onprem-db: 192.168.0.4                                            │
  │  PostgreSQL port 5432                                                 │
  │  → authenticates labuser                                              │
  │  → returns rows from "sales" table                                   │
  └─────────────────────────────────────────────────────────────────────┘
  
  Data flows back through the same path:
  on-prem → gateway VM → relay → serverless node → Spark DataFrame
  
  Then:
  DataFrame → NCC Private Endpoint → ADLS Gen2 (write as Delta)
```

---

## LAB 17 — Clean Up All Databricks Labs

```bash
# 1. Terminate all running clusters (from Databricks UI first)
#    Compute → each cluster → Terminate

# 2. Delete Databricks workspace
az databricks workspace delete \
  --resource-group rg-network-lab \
  --name dbr-workspace-lab \
  --yes

# 3. Delete managed resource group (auto-created by Databricks)
# Find its name:
az group list --query "[?contains(name,'databricks-rg')].[name]" -o tsv
# Then delete it:
az group delete --name databricks-rg-dbr-workspace-lab-XXXXXXX --yes

# 4. Delete VNets
az network vnet delete --resource-group rg-network-lab --name vnet-databricks
az network vnet delete --resource-group rg-network-lab --name vnet-gateway
az network vnet delete --resource-group rg-network-lab --name vnet-onprem

# 5. Delete ADLS
az storage account delete --resource-group rg-network-lab --name $ADLS_NAME --yes

# 6. Delete Service Principal
az ad sp delete --id $SP_APP_ID

# 7. Delete remaining resource group
az group delete --name rg-network-lab --yes --no-wait
```

---

## Summary — All Labs at a Glance

```
┌───────┬─────────────────────────────────────┬────────────┬────────────────────────┐
│ Lab   │ What You Build                       │ Cost       │ Key Concept            │
├───────┼─────────────────────────────────────┼────────────┼────────────────────────┤
│  1    │ VNet + 3 subnets                     │ FREE       │ Address spaces, CIDR   │
│  2    │ NSG + rules on subnets               │ FREE       │ Stateful firewall rules │
│  3    │ 2 VMs, test SSH/ping                 │ ~$0.05     │ Private IP routing     │
│  4    │ NAT Gateway (no public IP on VM)     │ ~$0.05     │ Stable outbound IP     │
│  5    │ VNet Peering (3 VNets, transitivity) │ ~$0.02     │ Non-transitive peering │
│  6    │ Private Endpoint for Storage         │ ~$0.02     │ Private IP for PaaS    │
│  7    │ Route Table (hub-spoke via VM)       │ ~$0.01     │ UDR, IP forwarding     │
│  8    │ DNS inside vs outside VNet           │ FREE       │ Private DNS override   │
│  9    │ Storage firewall scenarios           │ FREE       │ VNet rules, IP rules   │
│ 10    │ Clean up                             │ -          │ Cost hygiene           │
│ 11A-E │ Portal UI walkthroughs               │ FREE       │ Visual orientation     │
│ 12    │ Databricks VNet Injection            │ ~$0.10     │ Cluster VMs in YOUR VNet│
│ 13    │ Job Cluster → ADLS via PE            │ ~$0.15     │ OAuth, private storage │
│ 14    │ SQL Warehouse NCC + PE               │ ~$0.20     │ Serverless private data│
│ 15    │ Serverless Jobs networking           │ ~$0.10     │ NCC applies to jobs too│
│ 16    │ Private Network Gateway (on-prem)    │ ~$0.30     │ Serverless → on-prem  │
│ 17    │ Clean up all Databricks labs         │ -          │ Cost hygiene           │
└───────┴─────────────────────────────────────┴────────────┴────────────────────────┘

TOTAL if you run everything and delete promptly: ~$1.00–2.00
```
