$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Set-Location -LiteralPath $PSScriptRoot
# All runtime files stay inside this checkout; no system settings are changed.
New-Item -ItemType Directory -Force '.runtime', '.runtime/jars', '.runtime/hadoop/bin' | Out-Null
if (-not (Get-ChildItem '.runtime' -Directory -Filter 'jdk-17*')) {
    Invoke-WebRequest 'https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse' -OutFile '.runtime/java17.zip'
    Expand-Archive -LiteralPath '.runtime/java17.zip' -DestinationPath '.runtime' -Force
}
$jars = @{
    'delta-spark_2.12-3.2.1.jar' = 'io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar'
    'delta-storage-3.2.1.jar' = 'io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar'
    'antlr4-runtime-4.9.3.jar' = 'org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar'
}
foreach ($entry in $jars.GetEnumerator()) {
    $target = Join-Path '.runtime/jars' $entry.Key
    if (-not (Test-Path -LiteralPath $target)) {
        Invoke-WebRequest ('https://repo.maven.apache.org/maven2/' + $entry.Value) -OutFile $target
    }
}
# Community Windows builds of Apache Hadoop's native compatibility layer.
foreach ($name in @('winutils.exe', 'hadoop.dll')) {
    $target = Join-Path '.runtime/hadoop/bin' $name
    if (-not (Test-Path -LiteralPath $target)) {
        Invoke-WebRequest ('https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/' + $name) -OutFile $target
    }
}
if (-not (Test-Path '.venv/Scripts/python.exe')) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python virtual environment creation failed; use Python 3.11.' }
}
& ./.venv/Scripts/python.exe -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
Write-Output 'Setup complete. Run ./.venv/Scripts/python.exe -m pipelines.run --download --sample-rows 50000'
