param(
    [Parameter(Mandatory=$true)][string]$Before,
    [Parameter(Mandatory=$true)][string]$After,
    [string]$Prefix = '.tmp/profile-comparison',
    [int]$Items = 524288,
    [int]$Samples = 9
)
$ErrorActionPreference = 'Stop'
$workloads = @('roundtrip-1','roundtrip-4','roundtrip-8','roundtrip-16',
    'transfer-1p1c','transfer-4p4c','transfer-8p8c','transfer-1p7c','transfer-7p1c',
    'empty-1','empty-16')
$beforeRows = [Collections.Generic.List[object]]::new()
$afterRows = [Collections.Generic.List[object]]::new()
for ($index = 0; $index -lt $workloads.Count; $index++) {
    # Alternate which binary runs first; never run them simultaneously.
    $order = if ($index % 2 -eq 0) { @('before','after') } else { @('after','before') }
    foreach ($revision in $order) {
        $binary = if ($revision -eq 'before') { $Before } else { $After }
        $csv = & $binary --items $Items --samples $Samples --work $workloads[$index]
        if ($LASTEXITCODE -ne 0) { throw "$revision failed for $($workloads[$index])" }
        $rows = $csv | ConvertFrom-Csv
        foreach ($row in $rows) {
            if ($revision -eq 'before') { $beforeRows.Add($row) }
            else { $afterRows.Add($row) }
        }
        Write-Host "$revision $($workloads[$index]) completed"
    }
}
$beforeRows | Export-Csv -LiteralPath "$Prefix-before.csv" -NoTypeInformation -Encoding utf8
$afterRows | Export-Csv -LiteralPath "$Prefix-after.csv" -NoTypeInformation -Encoding utf8
