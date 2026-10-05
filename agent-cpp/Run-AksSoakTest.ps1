<#
.SYNOPSIS
    Runs and collects the C++ Agent release soak through the Linux jump box.

.DESCRIPTION
    Uses Azure VM Run Command only to execute kubectl on the configured jump box.
    Start deploys an isolated SigV4 Agent, verifies termination/restart recovery,
    and launches a sustained load Job. Status and Collect are read-only. Cleanup
    removes only resources labeled for this soak test.

.EXAMPLE
    .\agent-cpp\Run-AksSoakTest.ps1 -Action Start

.EXAMPLE
    .\agent-cpp\Run-AksSoakTest.ps1 -Action Collect
#>
[CmdletBinding()]
param(
    [ValidateSet("Start", "Status", "Collect", "Cleanup")]
    [string]$Action = "Status",
    [string]$JumpBoxResourceGroup = "FABRICPROXY",
    [string]$JumpBoxName = "fabricproxy001",
    [string]$Namespace = "fabric-shortcut-proxy",
    [int]$DurationSeconds = 14400,
    [int]$Concurrency = 4,
    [string]$ExpectedVersion = "cpp-1.0.0-rc.1",
    [string]$OutputDirectory = (Join-Path $PSScriptRoot ".soak-evidence")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($DurationSeconds -lt 60) {
    throw "DurationSeconds must be at least 60."
}
if ($Concurrency -lt 1 -or $Concurrency -gt 32) {
    throw "Concurrency must be between 1 and 32."
}
if ($Namespace -notmatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$") {
    throw "Namespace is not a valid Kubernetes namespace name."
}
if ($ExpectedVersion -notmatch "^cpp-[0-9A-Za-z.-]+$") {
    throw "ExpectedVersion contains unsupported characters."
}

function Invoke-JumpBox {
    param([Parameter(Mandatory)][string]$Script)

    $scriptPath = Join-Path ([IO.Path]::GetTempPath()) ("fsp-jumpbox-" + [guid]::NewGuid().ToString("N") + ".sh")
    try {
        $normalizedScript = $Script.Replace("`r`n", "`n")
        $encodedScript = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($normalizedScript))
        $wrapper = @"
set +e
remote_script=`$(mktemp)
printf '%s' '$encodedScript' | base64 -d > "`$remote_script"
bash "`$remote_script"
remote_exit=`$?
rm -f "`$remote_script"
printf '\n__FSP_REMOTE_EXIT=%s\n' "`$remote_exit"
exit 0
"@
        Set-Content -LiteralPath $scriptPath -Value $wrapper -Encoding ASCII
        $resultText = & az vm run-command invoke `
            --resource-group $JumpBoxResourceGroup `
            --name $JumpBoxName `
            --command-id RunShellScript `
            --scripts "@$scriptPath" `
            --output json 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Jump-box command failed:`n$($resultText | Out-String)"
        }
    }
    finally {
        Remove-Item -LiteralPath $scriptPath -Force -ErrorAction SilentlyContinue
    }
    $result = ($resultText | Out-String) | ConvertFrom-Json
    $message = ($result.value | ForEach-Object message) -join "`n"
    $stderr = ""
    if ($message -match "\[stderr\]\s*(?<error>.+)") {
        $stderr = $Matches.error.Trim()
        if ($stderr) {
            Write-Verbose $stderr
        }
    }
    $stdoutMatch = [regex]::Match($message, "(?s)\[stdout\]\s*(?<stdout>.*?)\s*\[stderr\]")
    if ($stdoutMatch.Success) {
        $stdout = $stdoutMatch.Groups["stdout"].Value.Trim()
        $exitMatch = [regex]::Match($stdout, "(?m)^__FSP_REMOTE_EXIT=(?<code>\d+)\s*$")
        if (-not $exitMatch.Success) {
            throw "Jump-box command did not report its remote exit code:`n$stdout"
        }
        $remoteExitCode = [int]$exitMatch.Groups["code"].Value
        $stdout = [regex]::Replace($stdout, "(?m)^__FSP_REMOTE_EXIT=\d+\s*$", "").Trim()
        if ($remoteExitCode -ne 0) {
            throw "Jump-box command exited with code $remoteExitCode.`n$stdout`n$stderr"
        }
        return $stdout
    }
    throw "Jump-box command returned an unexpected response:`n$message"
}

function Send-JumpBoxFile {
    param(
        [Parameter(Mandatory)][string]$LocalPath,
        [Parameter(Mandatory)][string]$RemotePath
    )

    $bytes = [IO.File]::ReadAllBytes($LocalPath)
    $encoded = [Convert]::ToBase64String($bytes)
    $script = "set -eu; install -d -m 755 /var/tmp/fsp-cpp-soak; printf '%s' '$encoded' | base64 -d > '$RemotePath'; chmod 644 '$RemotePath'"
    Invoke-JumpBox -Script $script | Out-Null
}

$kubectl = "sudo -u andreas -H kubectl"
$labelSelector = "fsp.microsoft.com/test=cpp-1-0-0-soak"

switch ($Action) {
    "Start" {
        $candidateImage = (Invoke-JumpBox -Script "set -eu; $kubectl -n '$Namespace' get deployment fsp-cpp-agent -o jsonpath='{.spec.template.spec.containers[0].image}'").Trim()
        $clientImage = (Invoke-JumpBox -Script "set -eu; $kubectl -n '$Namespace' get deployment fsp-manager -o jsonpath='{.spec.template.spec.containers[0].image}'").Trim()
        if ($candidateImage -notmatch "@sha256:[0-9a-f]{64}$") {
            throw "The running C++ deployment is not pinned by digest: $candidateImage"
        }
        if ($clientImage -notmatch "@sha256:[0-9a-f]{64}$") {
            throw "The running Python deployment is not pinned by digest: $clientImage"
        }

        $accessKey = "FSPSOAK" + ([guid]::NewGuid().ToString("N").Substring(0, 16).ToUpperInvariant())
        $secretBytes = New-Object byte[] 32
        $randomNumberGenerator = [Security.Cryptography.RandomNumberGenerator]::Create()
        try {
            $randomNumberGenerator.GetBytes($secretBytes)
        }
        finally {
            $randomNumberGenerator.Dispose()
        }
        $secretKey = [Convert]::ToBase64String($secretBytes)
        $accessKeyData = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($accessKey))
        $secretKeyData = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($secretKey))
        $activeDeadline = $DurationSeconds + 900

        $workload = @"
apiVersion: v1
kind: Secret
metadata:
  name: fsp-cpp-soak-auth
  namespace: $Namespace
  labels:
    fsp.microsoft.com/test: cpp-1-0-0-soak
type: Opaque
data:
  S3_ACCESS_KEY_ID: $accessKeyData
  S3_SECRET_ACCESS_KEY: $secretKeyData
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fsp-cpp-soak-agent
  namespace: $Namespace
  labels:
    fsp.microsoft.com/test: cpp-1-0-0-soak
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: fsp-cpp-soak-agent
  template:
    metadata:
      labels:
        app.kubernetes.io/name: fsp-cpp-soak-agent
        fsp.microsoft.com/test: cpp-1-0-0-soak
    spec:
      imagePullSecrets:
        - name: acr-pull
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile:
          type: RuntimeDefault
      initContainers:
        - name: seed
          image: $clientImage
          command:
            - python
            - -c
            - from pathlib import Path; p=Path('/artifacts/benchmark/payload.bin'); p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(b'\0' * 65536)
          volumeMounts:
            - name: artifacts
              mountPath: /artifacts
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
      containers:
        - name: agent
          image: $candidateImage
          env:
            - name: HOST
              value: 0.0.0.0
            - name: PORT
              value: "9400"
            - name: STORE_DIR
              value: /artifacts
            - name: INDEX_FILE
              value: /var/lib/fsp/objects.index
            - name: REQUIRE_GENERATION
              value: "0"
            - name: INDEX_REFRESH_SECONDS
              value: "0"
            - name: AGENT_DRAIN_GRACE_SECONDS
              value: "15"
            - name: S3_AUTH_MODE
              value: sigv4
            - name: S3_BUCKET
              value: fsp-soak
            - name: S3_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: fsp-cpp-soak-auth
                  key: S3_ACCESS_KEY_ID
            - name: S3_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: fsp-cpp-soak-auth
                  key: S3_SECRET_ACCESS_KEY
            - name: MANAGER_URL
              value: ""
            - name: MATERIALIZE_MODE
              value: eager
            - name: AGENT_ID
              value: aks-release-soak
          ports:
            - name: http
              containerPort: 9400
          startupProbe:
            httpGet:
              path: /healthz
              port: http
            failureThreshold: 30
            periodSeconds: 2
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
            periodSeconds: 2
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            failureThreshold: 3
            periodSeconds: 10
          resources:
            requests:
              cpu: 100m
              memory: 64Mi
            limits:
              cpu: "2"
              memory: 512Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: artifacts
              mountPath: /artifacts
              readOnly: true
            - name: index
              mountPath: /var/lib/fsp
      volumes:
        - name: artifacts
          emptyDir: {}
        - name: index
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: fsp-cpp-soak-agent
  namespace: $Namespace
  labels:
    fsp.microsoft.com/test: cpp-1-0-0-soak
spec:
  selector:
    app.kubernetes.io/name: fsp-cpp-soak-agent
  ports:
    - name: http
      port: 80
      targetPort: http
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fsp-cpp-soak-agent
  namespace: $Namespace
  labels:
    fsp.microsoft.com/test: cpp-1-0-0-soak
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: fsp-cpp-soak-agent
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: fsp-cpp-soak-client
      ports:
        - protocol: TCP
          port: 9400
"@

        $job = @"
apiVersion: batch/v1
kind: Job
metadata:
  name: fsp-cpp-soak
  namespace: $Namespace
  labels:
    fsp.microsoft.com/test: cpp-1-0-0-soak
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $activeDeadline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: fsp-cpp-soak-client
        fsp.microsoft.com/test: cpp-1-0-0-soak
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - name: acr-pull
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: load
          image: $clientImage
          command:
            - python
            - /scripts/soak_aks.py
            - --host
            - fsp-cpp-soak-agent
            - --bucket
            - fsp-soak
            - --object-key
            - benchmark/payload.bin
            - --duration
            - "$DurationSeconds"
            - --concurrency
            - "$Concurrency"
            - --object-size
            - "65536"
          env:
            - name: S3_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: fsp-cpp-soak-auth
                  key: S3_ACCESS_KEY_ID
            - name: S3_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: fsp-cpp-soak-auth
                  key: S3_SECRET_ACCESS_KEY
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: "2"
              memory: 512Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: script
              mountPath: /scripts
              readOnly: true
      volumes:
        - name: script
          configMap:
            name: fsp-cpp-soak-client
"@

        $temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("fsp-cpp-soak-" + [guid]::NewGuid().ToString("N"))
        New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
        try {
            $workloadPath = Join-Path $temporaryDirectory "workload.yaml"
            $jobPath = Join-Path $temporaryDirectory "job.yaml"
            Set-Content -LiteralPath $workloadPath -Value $workload -Encoding UTF8
            Set-Content -LiteralPath $jobPath -Value $job -Encoding UTF8
            Send-JumpBoxFile -LocalPath (Join-Path $PSScriptRoot "soak_aks.py") -RemotePath "/var/tmp/fsp-cpp-soak/soak_aks.py"
            Send-JumpBoxFile -LocalPath $workloadPath -RemotePath "/var/tmp/fsp-cpp-soak/workload.yaml"
            Send-JumpBoxFile -LocalPath $jobPath -RemotePath "/var/tmp/fsp-cpp-soak/job.yaml"
        }
        finally {
            Remove-Item -Recurse -Force $temporaryDirectory
        }

        $startScript = @'
set -eu
K="sudo -u andreas -H kubectl"
NS="{NAMESPACE}"
$K -n "$NS" delete job fsp-cpp-soak --ignore-not-found
$K -n "$NS" create configmap fsp-cpp-soak-client \
  --from-file=soak_aks.py=/var/tmp/fsp-cpp-soak/soak_aks.py \
  --dry-run=client -o yaml | $K apply -f -
$K -n "$NS" label configmap fsp-cpp-soak-client fsp.microsoft.com/test=cpp-1-0-0-soak --overwrite
$K apply -f /var/tmp/fsp-cpp-soak/workload.yaml
$K -n "$NS" rollout status deployment/fsp-cpp-soak-agent --timeout=300s
version=$($K -n "$NS" exec deployment/fsp-cpp-soak-agent -- /usr/local/bin/fsp-cpp-agent --version)
test "$version" = "{EXPECTED_VERSION}"
old_pod=$($K -n "$NS" get pod -l app.kubernetes.io/name=fsp-cpp-soak-agent -o jsonpath='{.items[0].metadata.name}')
started=$(date +%s)
$K -n "$NS" delete pod "$old_pod" --wait=false
deadline=$((started + 300))
new_pod=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  new_pod=$($K -n "$NS" get pod -l app.kubernetes.io/name=fsp-cpp-soak-agent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  ready=$($K -n "$NS" get pod "$new_pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  if [ -n "$new_pod" ] && [ "$new_pod" != "$old_pod" ] && [ "$ready" = "True" ]; then
    break
  fi
  sleep 2
done
if [ -z "$new_pod" ] || [ "$new_pod" = "$old_pod" ] || [ "$ready" != "True" ]; then
  echo "candidate did not recover from restart within 300 seconds" >&2
  exit 1
fi
recovery_seconds=$(($(date +%s) - started))
$K -n "$NS" create configmap fsp-cpp-soak-metadata \
  --from-literal=candidate-image="{CANDIDATE_IMAGE}" \
  --from-literal=expected-version="{EXPECTED_VERSION}" \
  --from-literal=restart-recovery-seconds="$recovery_seconds" \
  --from-literal=started-at="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --dry-run=client -o yaml | $K apply -f -
$K -n "$NS" label configmap fsp-cpp-soak-metadata fsp.microsoft.com/test=cpp-1-0-0-soak --overwrite
$K apply -f /var/tmp/fsp-cpp-soak/job.yaml
echo "candidate_version=$version"
echo "candidate_image={CANDIDATE_IMAGE}"
echo "restart_recovery_seconds=$recovery_seconds"
$K -n "$NS" get deployment fsp-cpp-soak-agent
$K -n "$NS" get job fsp-cpp-soak
'@
        $startScript = $startScript.Replace("{NAMESPACE}", $Namespace).
            Replace("{EXPECTED_VERSION}", $ExpectedVersion).
            Replace("{CANDIDATE_IMAGE}", $candidateImage)
        Invoke-JumpBox -Script $startScript
    }
    "Status" {
        $statusScript = "set -eu; $kubectl -n '$Namespace' get configmap fsp-cpp-soak-metadata -o yaml 2>/dev/null || true; $kubectl -n '$Namespace' get deployment,job,pod -l '$labelSelector' -o wide 2>/dev/null || true; $kubectl -n '$Namespace' logs job/fsp-cpp-soak --tail=5 2>/dev/null || true"
        Invoke-JumpBox -Script $statusScript
    }
    "Collect" {
        New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
        $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
        $outputPath = Join-Path $OutputDirectory "cpp-1.0.0-rc.1-aks-soak-$timestamp.txt"
        $collectScript = "set -eu; echo '--- METADATA ---'; $kubectl -n '$Namespace' get configmap fsp-cpp-soak-metadata -o yaml; echo '--- RESOURCES ---'; $kubectl -n '$Namespace' get deployment,job,pod -l '$labelSelector' -o wide; echo '--- POD STATUS ---'; $kubectl -n '$Namespace' get pods -l '$labelSelector' -o json; echo '--- AGENT LOGS ---'; $kubectl -n '$Namespace' logs deployment/fsp-cpp-soak-agent; echo '--- SOAK LOGS ---'; $kubectl -n '$Namespace' logs job/fsp-cpp-soak"
        $evidence = Invoke-JumpBox -Script $collectScript
        Set-Content -LiteralPath $outputPath -Value $evidence -Encoding UTF8
        if ($evidence -notmatch "FINAL_RESULT=(?<json>\{[^\r\n]+\})") {
            throw "No final soak result was found. Evidence was saved to $outputPath."
        }
        $result = $Matches.json | ConvertFrom-Json
        if (-not $result.passed -or $result.request_errors -ne 0) {
            throw "The soak release gate failed. Evidence was saved to $outputPath."
        }
        if ($evidence -match '"reason"\s*:\s*"OOMKilled"') {
            throw "An OOMKilled container was found. Evidence was saved to $outputPath."
        }
        Write-Host "Soak release gate passed. Evidence: $outputPath"
    }
    "Cleanup" {
        $cleanupScript = "set -eu; $kubectl -n '$Namespace' delete deployment,service,job,secret,configmap,networkpolicy -l '$labelSelector' --ignore-not-found"
        Invoke-JumpBox -Script $cleanupScript
    }
}
