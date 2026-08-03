"""Enterprise (scale-out) components for Fabric Shortcut Proxy.

Shipped as the separate ``fabric-shortcut-proxy-enterprise`` distribution, which
depends on the Lite ``fabric-shortcut-proxy`` core. Importing anything here means
you are running the clustered topology (Manager control plane, agent link,
retention GC, external-LB renderer). The Lite core never imports this package.
"""
