#Requires -Version 5.1
<#
.SYNOPSIS
  Bootstrap local git repo for the BIS Databricks project and push to GitHub.
.DESCRIPTION
  Run once from PowerShell. Idempotent: skips git init if already a repo.
  Steps: init -> main branch -> add -> initial commit -> add remote -> push.
.PARAMETER RemoteUrl
  Your GitHub remote, e.g. https://github.com/<you>/bis-databricks.git
.EXAMPLE
  .\scripts\git_bootstrap.ps1 -RemoteUrl "https://github.com/<you>/bis-databricks.git"
#>
param(
  [Parameter(Mandatory = $true)]
  [string]$RemoteUrl
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot

Set-Location -LiteralPath $RepoRoot

# 1. Preconditions
try { git --version | Out-Null } catch { throw "git not found on PATH. Install Git for Windows first." }

# 2. Init (skip if already a repo)
if (Test-Path -LiteralPath (Join-Path $RepoRoot ".git")) {
  Write-Host "Already a git repo, skipping init."
} else {
  git init
  git branch -M main
  Write-Host "Initialized repo on branch main."
}

# 3. Stage + commit (skip if nothing to commit)
git add -A
$status = git status --porcelain
if ([string]::IsNullOrWhiteSpace($status)) {
  Write-Host "Nothing to commit."
} else {
  git commit -m "BIS cashless medallion pipeline: bronze/silver/gold + annual refresh job"
  Write-Host "Committed."
}

# 4. Remote (add or verify)
$existing = git remote get-url origin 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($existing)) {
  git remote add origin $RemoteUrl
  Write-Host "Added remote origin -> $RemoteUrl"
} elseif ($existing -ne $RemoteUrl) {
  Write-Host "Remote origin already set to $existing (requested $RemoteUrl). Update manually if needed:"
  Write-Host "  git remote set-url origin $RemoteUrl"
} else {
  Write-Host "Remote origin already correct."
}

# 5. Push
git push -u origin main
Write-Host "Pushed. Next: Databricks > Git Folders > Add Git Folder > paste the same URL."
