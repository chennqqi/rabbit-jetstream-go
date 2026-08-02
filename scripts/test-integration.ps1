$ErrorActionPreference = 'Stop'
$name = "rabbit-jetstream-go-test-$PID"

try {
    docker run --rm -d --name $name -p 127.0.0.1::4222 nats:2.14.1-alpine -js | Out-Null
    $port = docker port $name 4222/tcp
    $port = ($port -split ':')[-1]
    $ready = $false
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        if (Test-NetConnection -ComputerName 127.0.0.1 -Port $port -InformationLevel Quiet -WarningAction SilentlyContinue) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw 'NATS did not become ready' }
    $env:NATS_URL = "nats://127.0.0.1:$port"
    go test -tags=integration -count=1 ./...
} finally {
    docker stop $name 2>$null | Out-Null
}
