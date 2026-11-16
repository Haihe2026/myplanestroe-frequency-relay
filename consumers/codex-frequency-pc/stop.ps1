$ErrorActionPreference = 'Stop'
Set-Content -LiteralPath (Join-Path $PSScriptRoot 'STOP') -Value 'operator requested stop' -Encoding UTF8
Write-Output 'Frequency consumer will stop after its current bounded poll. Image relay is untouched.'
