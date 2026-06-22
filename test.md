flowchart LR
    %% Styles
    classDef dbx fill:#FFEEE8,stroke:#FF6B3D,stroke-width:2px,color:#222;
    classDef control fill:#FFF4CC,stroke:#D4A72C,stroke-width:2px,color:#222;
    classDef azure fill:#E8F1FF,stroke:#3B82F6,stroke-width:2px,color:#222;
    classDef network fill:#EAFBF1,stroke:#22A06B,stroke-width:2px,color:#222;
    classDef warning fill:#FFF1F0,stroke:#D92D20,stroke-width:2px,color:#222;
    classDef neutral fill:#F4F5F7,stroke:#98A2B3,stroke-width:1.5px,color:#222;

    A[Databricks Serverless<br/>Notebook / Job / SQL]:::dbx
    B[NCC<br/>Network Connectivity Configuration]:::control
    C[Private Network Gateway<br/>PNG]:::control

    subgraph VNET[Customer Azure VNet]
        D[Delegated PNG Subnet<br/>/28 or larger]:::azure
        E[Private DNS Resolver]:::network
        F[Private DB / API on VM]:::network
        G[Firewall / NAT / Security Appliance]:::network
        H[ExpressRoute / VPN]:::network
    end

    I[On-prem Database / Service]:::network
    J[Azure SQL / Storage / PaaS]:::warning
    K[Private Endpoint / Service Endpoint Path]:::warning
    L[Default Databricks Egress]:::neutral

    A --> B --> C --> D
    C --> E
    D --> F
    D --> G
    D --> H --> I

    A -. PaaS request .-> J
    J --> K
    A -. unmatched traffic .-> L

    M[SPECIFIC_DESTINATIONS<br/>Only listed FQDNs use PNG]:::neutral
    N[ALL_TRAFFIC<br/>Most outbound traffic uses PNG<br/>except PE / SE matches]:::neutral

    B --- M
    B --- N
