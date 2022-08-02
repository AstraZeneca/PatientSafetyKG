# Refresh load files inside the docker container
# FOR LOCAL TESTING ONLY!
param (
    [string]$source_dir = "$PSScriptRoot\..\..\local_test_archive",
    [string]$load_script = "$PSScriptRoot\..\load_data\load.cypher",
    [string]$container_path = "pskg:/var/lib/neo4j/import"
)

# Copy import files to docker container
Write-Host "Copying import files from ${source_dir}"

foreach ($file in Get-ChildItem $source_dir ) {
    Write-Host "Copying ${file} to ${container_path}"
    $linux_file = $file.FullName -replace '\\','/'
    docker cp $linux_file $container_path  
}

$load_script_file = Get-Item $load_script
Write-Host "Copying ${load_script_file} to ${container_path}"
$linux_file = $load_script_file.FullName -replace '\\','/'
docker cp $linux_file $container_path
