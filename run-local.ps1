# Corre el bot UNA vez en local (sin Docker) para la hoja indicada y termina.
# Uso:  .\run-local.ps1            -> TRIJAZ
#       .\run-local.ps1 V1         -> REDONDO V1 (usa service-account.json)
param([string]$Hoja = "TRIJAZ")
$keyfiles = @{ TRIJAZ="credentials_multicity.json"; V1="service-account.json"; V2="credentials_v2.json"; V3="credentials_v3.json";
               PUEBLA="credentials_puebla.json"; JALISCO="credentials_jalisco.json"; EDOMEX="credentials_edomex.json"; YUCATAN="credentials_yucatan.json" }
$env:PRIORIDAD_PROCESO = $Hoja
$env:GOOGLE_KEYFILE    = "$PSScriptRoot\credentials\$($keyfiles[$Hoja])"
$env:SS_ENTITY_CACHE   = "$PSScriptRoot\data\entity_cache.json"
$env:SS_LOCKFILE       = "$PSScriptRoot\data\.script.lock"
$env:SS_LOG_FILE       = "$PSScriptRoot\logs\local.log"
$env:LOOP_ENABLED      = "false"
& "$PSScriptRoot\.venv\Scripts\python.exe" "$PSScriptRoot\apiskyscanner_api.py"
