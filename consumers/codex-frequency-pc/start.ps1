$ErrorActionPreference = 'Stop'
$taskStopMarker = Join-Path $PSScriptRoot 'STOP'
if (Test-Path -LiteralPath $taskStopMarker) {
    if ((Get-Item -LiteralPath $taskStopMarker).PSIsContainer) { throw 'STOP must be a file' }
    Remove-Item -LiteralPath $taskStopMarker
}
$taskPython = (Get-Command python.exe -ErrorAction Stop).Source
$taskPythonW = Join-Path (Split-Path -Parent $taskPython) 'pythonw.exe'
if (-not (Test-Path -LiteralPath $taskPythonW -PathType Leaf)) { throw 'pythonw.exe is unavailable' }
Start-Process -FilePath $taskPythonW -ArgumentList ('"' + (Join-Path $PSScriptRoot 'watch_frequency_consumer.py') + '"') -WorkingDirectory $PSScriptRoot -WindowStyle Hidden
Write-Output 'Frequency consumer start requested; its own lock prevents duplicate instances.'
