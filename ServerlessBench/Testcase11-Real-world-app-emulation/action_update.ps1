param([string]$sequenceID, [string]$functionID)

$FUNCTION_DIR = "."
$CDF_DIR = "./CDFs"

# 妫€鏌ュ繀瑕佹枃浠?
if (-not (Test-Path "$FUNCTION_DIR\function.c")) {
    Write-Error "function.c not found in $FUNCTION_DIR"
    exit 1
}

# 缂栬瘧 function.c锛堥渶瑕?gcc锛?
& gcc "$FUNCTION_DIR\function.c" --static -o "$FUNCTION_DIR\function.exe"

# 鍒涘缓 zip 鏂囦欢
$filesToZip = @(
    "$FUNCTION_DIR\__main__.py",
    "$FUNCTION_DIR\function.exe",
    "$FUNCTION_DIR\utils.py",
    "$CDF_DIR\memCDF.csv",
    "$CDF_DIR\execTimeCDF.csv"
)

$zipPath = "function.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath }

# 浣跨敤 Compress-Archive锛圥owerShell 鍐呯疆锛?
Compress-Archive -Path $filesToZip -DestinationPath $zipPath -Force

# 鍒涘缓 OpenWhisk 鍑芥暟
$funcName = "func$sequenceID-$functionID"
& wsk -i action update $funcName function.zip --docker lqyuan980413/realworldemulate:0.1

if ($LASTEXITCODE -eq 0) {
    Write-Host "Function $funcName created successfully" -ForegroundColor Green
} else {
    Write-Host "Failed to create function $funcName" -ForegroundColor Red
}
