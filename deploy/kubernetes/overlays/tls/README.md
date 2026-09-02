# Manager TLS overlay

This overlay encrypts Agent-to-Manager control traffic. The Manager serves HTTPS
on port 9443. Python materializers verify the configured CA directly. C++ Agents
send control calls to a loopback `stunnel` sidecar, which verifies the Manager
certificate and opens the TLS connection.

Create a certificate whose SAN includes `fsp-manager` before applying the overlay.
For a development cluster, create a local CA and certificate outside the repository:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout ca.key -out ca.crt -subj '/CN=fsp-manager-ca'
openssl req -newkey rsa:2048 -nodes -keyout tls.key -out tls.csr \
  -subj '/CN=fsp-manager'
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out tls.crt -days 30 -extfile <(printf 'subjectAltName=DNS:fsp-manager')
kubectl -n fabric-shortcut-proxy create secret generic fsp-manager-tls \
  --from-file=tls.crt --from-file=tls.key --from-file=ca.crt
```

Use cert-manager or an organization-managed issuer for production certificates.
Apply the overlay after creating `fsp-source` and `fsp-manager-tls`:

```sh
kubectl apply -k deploy/kubernetes/overlays/tls
```

The control channel still uses Manager Basic authentication. Configure
`MANAGER_AUTH_USERNAME` and `MANAGER_AUTH_PASSWORD` in `fsp-source`; the
certificate Secret contains only TLS material.