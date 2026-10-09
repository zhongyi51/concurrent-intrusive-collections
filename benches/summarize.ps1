param(
    [string]$InputCsv = '.tmp/queue-benchmark.csv',
    [string]$OutputCsv = '.tmp/queue-summary.csv'
)
$ErrorActionPreference = 'Stop'
$rows = Import-Csv -LiteralPath $InputCsv
$summary = @($rows | Group-Object mode, workload, implementation, shards | ForEach-Object {
    $first = $_.Group[0]
    $rates = @($_.Group | ForEach-Object {
        [double]$_.operations * 1000 / [double]$_.nanoseconds
    } | Sort-Object)
    if ($rates.Count % 2 -eq 0) { throw 'Use an odd sample count for this report.' }
    $middle = [int][math]::Floor($rates.Count / 2)
    [pscustomobject]@{
        mode = $first.mode
        workload = $first.workload
        implementation = $first.implementation
        shards = [int]$first.shards
        samples = $rates.Count
        median_mops = $rates[$middle]
        p25_mops = $rates[[int][math]::Floor(($rates.Count - 1) * 0.25)]
        p75_mops = $rates[[int][math]::Floor(($rates.Count - 1) * 0.75)]
        speedup_vs_ms = 1.0
    }
})
foreach ($row in $summary) {
    $baseline = $summary | Where-Object {
        $_.mode -eq $row.mode -and $_.workload -eq $row.workload -and $_.implementation -eq 'ms'
    }
    if (@($baseline).Count -ne 1) { throw 'Each workload needs exactly one MS baseline.' }
    $row.speedup_vs_ms = $row.median_mops / $baseline.median_mops
}
$summary | Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding utf8
$summary | Format-Table mode, workload, implementation, shards,
    @{n='Mops/s';e={'{0:F2}' -f $_.median_mops}},
    @{n='P25';e={'{0:F2}' -f $_.p25_mops}},
    @{n='P75';e={'{0:F2}' -f $_.p75_mops}},
    @{n='Ratio';e={'{0:F2}' -f $_.speedup_vs_ms}} -AutoSize
