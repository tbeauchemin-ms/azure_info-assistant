<#
.SYNOPSIS
    Create or update an Azure Cognitive Search data source, index, and indexer via REST with robust error handling.

.PARAMETER ResourceGroupName
    Name of the Azure Resource Group where the Azure AI Search Service is deployed.

.PARAMETER SearchServiceName
    Name of the Azure AI Search Service.

.PARAMETER OpenAIServiceName
    Name of the Azure OpenAI Service used for vectorization.

.PARAMETER DataSourceName
    Name of the data source to create or update. Default: 'default-datasource'.

.PARAMETER StorageType
    The type of storage the Search Service will be connecting to. Allowed values: 'azureblob'. Default: 'azureblob'.

.PARAMETER StorageConnString
    Full connection string used for the Search Service to connect to the Storage. Required if using connection string authentication.

.PARAMETER StorageManagedIdentity
    The Resource Id of the Managed Identity used for the Search Service to connect to the Storage. Required if using managed identity authentication.

.PARAMETER StorageContainerName
    Name of the Blob Container to point this data source at when the Storage Type is 'azureblob'.

.PARAMETER IndexName
    Name of the search index to create or update. Default: 'default-index'.

.PARAMETER IndexerName
    Name of the indexer to create or update. Default: 'default-indexer'.

.PARAMETER SemanticConfigName
    Name of the semantic configuration for the index. Default: 'default-semantic-config'.

.PARAMETER VectorProfileName
    Name of the vector search profile. Default: 'default-vector-profile'.

.PARAMETER VectorAlgorithmName
    Name of the vector search algorithm. Default: 'default-vector-algorithm'.

.PARAMETER VectorizerName
    Name of the vectorizer. Default: 'default-vectorizer'.

.PARAMETER VectorizerType
    Type of the vectorizer. Allowed value: 'azureOpenAI'. Default: 'azureOpenAI'.

.PARAMETER EmbeddingModel
    Name of the Azure OpenAI Embedding Model. Default: 'text-embedding-ada-002'.

.PARAMETER MessageLevel
    Controls the verbosity of script output. Allowed values: 'Debug', 'Verbose', 'Information', 'Warning', 'Error'. Default: 'Information'.

.PARAMETER WhatIf
    If specified, shows what would happen if the command runs, without making any changes.

#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string] $ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string] $SearchServiceName,

    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string] $OpenAIServiceName,

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $DataSourceName = 'default-datasource',

    [Parameter(Mandatory=$false)]
    [ValidateSet('azureblob')]
    [string] $StorageType = 'azureblob',

    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string] $StorageConnString,

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $StorageManagedIdentity,

    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string] $StorageContainerName,

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $IndexName = 'default-index',

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $IndexerName = 'default-indexer',

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $SemanticConfigName = 'default-semantic-config',

    [parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $VectorProfileName = 'default-vector-profile',

    [parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $VectorAlgorithmName = 'default-vector-algorithm',

    [parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $VectorizerName = 'default-vectorizer',

    [Parameter(Mandatory=$false)]
    [ValidateSet('azureOpenAI')]
    [string] $VectorizerType = 'azureOpenAI',

    [Parameter(Mandatory=$false)]
    [ValidateNotNullOrEmpty()]
    [string] $EmbeddingModel = 'text-embedding-ada-002',

    [Parameter(Mandatory=$false)]
    [ValidateSet("Debug", "Verbose", "Information", "Warning", "Error")]
    [string] $MessageLevel = "Information",

    [Parameter(Mandatory=$false)]
    [switch] $WhatIf

)

# Set output preferences based on MessageLevel
switch ($MessageLevel) {
    "Debug" {
        $DebugPreference = "Continue"
        $VerbosePreference = "Continue"
        $InformationPreference = "Continue"
        $WarningPreference = "Continue"
        $ErrorActionPreference = "Continue"
    }
    "Verbose" {
        $DebugPreference = "SilentlyContinue"
        $VerbosePreference = "Continue"
        $InformationPreference = "Continue"
        $WarningPreference = "Continue"
        $ErrorActionPreference = "Continue"
    }
    "Information" {
        $DebugPreference = "SilentlyContinue"
        $VerbosePreference = "SilentlyContinue"
        $InformationPreference = "Continue"
        $WarningPreference = "Continue"
        $ErrorActionPreference = "Continue"
    }
    "Warning" {
        $DebugPreference = "SilentlyContinue"
        $VerbosePreference = "SilentlyContinue"
        $InformationPreference = "SilentlyContinue"
        $WarningPreference = "Continue"
        $ErrorActionPreference = "Continue"
    }
    "Error" {
        $DebugPreference = "SilentlyContinue"
        $VerbosePreference = "SilentlyContinue"
        $InformationPreference = "SilentlyContinue"
        $WarningPreference = "SilentlyContinue"
        $ErrorActionPreference = "Continue"
    }
    default {
        $DebugPreference = "SilentlyContinue"
        $VerbosePreference = "SilentlyContinue"
        $InformationPreference = "Continue"
        $WarningPreference = "Continue"
        $ErrorActionPreference = "Continue"
    }
}

# Ensure Az modules are imported
Import-Module Az.Search -ErrorAction Stop

function Get-SearchServiceAdminKey {
    param(
        [Parameter(Mandatory=$true)]
        [string] $ResourceGroupName,

        [Parameter(Mandatory=$true)]
        [string] $SearchServiceName
    )

    $adminKeys = $null

    Write-Information "Retrieving admin key for Search Service '$($SearchServiceName)' in Resource Group '$($ResourceGroupName)'."

    # Use Az PowerShell module to get the admin key
    try {
        Write-Debug "Executing: Get-AzSearchAdminKeyPair -ResourceGroupName $($ResourceGroupName) -ServiceName $($SearchServiceName)"
        $adminKeys = Get-AzSearchAdminKeyPair -ResourceGroupName $ResourceGroupName -ServiceName $SearchServiceName
    } catch {
        Write-Error "Failed to retrieve admin key for Search Service '$SearchServiceName' in Resource Group '$ResourceGroupName'."
        return $null
    }

    return $adminKeys.Primary

}

function Invoke-AzureSearchApi {
    param(
        [ValidateSet('Get','Post','Put','Delete')]
        [string] $Method = 'Get',

        [Parameter(Mandatory)]
        [string] $Url,

        [Parameter(Mandatory)]
        [hashtable] $Headers,

        [Parameter(Mandatory=$false)]
        [string] $Body = $null
    )

    Write-Information "Invoking Azure Search API: $($Method) $($Url)"
    Write-Debug " Headers: $($Headers | Out-String)"
    Write-Debug " Body: $($Body | Out-String)"

    try {
        if ($Body) {
            $resp = Invoke-WebRequest -Method $Method -Uri $Url -Headers $Headers -Body $Body -UseBasicParsing -ErrorAction SilentlyContinue
        } else {
            $resp = Invoke-WebRequest -Method $Method -Uri $Url -Headers $Headers -UseBasicParsing -ErrorAction SilentlyContinue
        }
        Write-Debug " Response: $($resp | Out-String)"
    }
    catch {
        Write-Error "Failed to invoke Azure Search API: $($Method) $($Url). Error: $_"
        return $null
    }

    # Check status
    if ($null -eq $resp -or $resp.StatusCode -lt 200 -or $resp.StatusCode -ge 300) {
        $status = if ($resp) { $resp.StatusCode } else { 'NoResponse' }
        Write-Error "HTTP $status returned from $Url"

        # Dump raw content (or full exception) for debugging
        if ($resp -and $resp.Content) {
            $resp.Content | Out-File -FilePath $errorFile -Encoding UTF8
            Write-Host "Error details written to $errorFile"
        } else {
            "No response content; raw object:`n$resp" | Out-File -FilePath $errorFile -Encoding UTF8
            Write-Host "No content; full response object written to $errorFile"
        }

        exit 1
    }

    return $resp

}

function Update-DataSource {
    [OutputType([bool])]
    param ()

    $objDataSource = @{
        name = ''
        type = ''
        credentials = @{
            'connectionString' = ''
        }
    }

    $objDataSource.name = $DataSourceName
    if ($StorageType -eq 'azureblob') {
        $objDataSource.type = 'azureblob'
        $objDataSource.credentials.connectionString = $StorageConnString
        $objDataSource.container = @{
            name = $StorageContainerName
        }
        if ($StorageManagedIdentity) {
            $objDataSource.identity = @{
                '@odata.type' = '#Microsoft.Azure.Search.DataUserAssignedIdentity'
                userAssignedIdentity = $StorageManagedIdentity
            }
        }
    } else {
        throw "[Create-Datasource] Unsupported Storage Type: $StorageType"
    }

    $strApiVersion = '2025-05-01-preview'
    $strBaseUrl = "https://$($SearchServiceName).search.windows.net"
    $strDataSourceUrl  = "$($strBaseUrl)/datasources('$($DataSourceName)')?api-version=$($strApiVersion)"
    $strDataSourceJson = $objDataSource | ConvertTo-Json -Depth 20
    if ($WhatIf) {
        Write-Host "Data Source '$($DataSourceName)' would have been created/updated with at '$($strDataSourceUrl)':"
        Write-Host $strDataSourceJson
        return $true
    }

    try {
        $strHeaders = @{
            'api-key' = (Get-SearchServiceAdminKey -ResourceGroupName $ResourceGroupName -SearchServiceName $SearchServiceName)
            'Content-Type' = 'application/json'
        }
        Invoke-AzureSearchApi -Method Put -Url $strDataSourceUrl -Headers $strHeaders -Body $strDataSourceJson
    } catch {
        Write-Error "Error invoking API to create or update Data Source '$DataSourceName'."
        return $false
    }

    return $true

}

function Update-Index {
    [OutputType([bool])]
    param ()

    # Build Fields for Index
    $objFields = @(
        @{
            name = "id"
            type = "Edm.String"
            searchable = $false
            filterable = $false
            retrievable = $true
            stored = $true
            sortable = $false
            facetable = $false
            key = $true
            synonymMaps = @()
        },
        @{
            name = "url"
            type = "Edm.String"
            searchable = $true
            filterable = $true
            retrievable = $true
            stored = $true
            sortable = $false
            facetable = $false
            key = $false
            analyzer = "standard.lucene"
            synonymMaps = @()
        },
        @{
            name = "content"
            type = "Edm.String"
            searchable = $true
            filterable = $false
            retrievable = $true
            stored = $true
            sortable = $false
            facetable = $false
            key = $false
            analyzer = "standard.lucene"
            synonymMaps = @()
        },
        @{
            name = "last_modified"
            type = "Edm.DateTimeOffset"
            searchable = $false
            filterable = $true
            retrievable = $true
            stored = $true
            sortable = $false
            facetable = $false
            key = $false
            synonymMaps = @()
        }
    )

    # Build Semantic Configuration
    $objSemanticConfig = @{
        configurations = @(
            @{
                name = $SemanticConfigName
                prioritizedFields = @{
                    titleField = @{
                        fieldName = 'url'
                    }
                    prioritizedContentFields = @(
                        @{
                            fieldName = 'content'
                        }
                    )
                    prioritizedKeywordsFields = @(
                        @{
                            fieldName = 'url'
                        }
                    )
                }
            }
        )
    }

    # Build Vector Search Profile
    $objVectorSearchProfile = @{
        algorithms = @(
            @{
                name = $VectorAlgorithmName
                kind = 'hnsw'
                hnswParameters = @{
                    efConstruction = 400
                    efSearch = 500
                    m = 6
                    metric = 'cosine'
                }
            }
        )
        compressions = @()
        profiles = @(
            @{
                name = $VectorProfileName
                algorithm = $VectorAlgorithmName
                vectorizer = $VectorizerName
            }
        )
        vectorizers = @(
            @{
                name = $VectorizerName
                kind = $VectorizerType
                azureOpenAIParameters = @{
                    resourceUri  = "https://$($OpenAIServiceName).openai.azure.com"
                    apiKey       = $null
                    deploymentId = $EmbeddingModel
                    authIdentity = $null
                    modelName    = $EmbeddingModel
                }
            }
        )
    }

    $objIndex = @{
        name = $IndexName
        fields = $objFields
        semantic = $objSemanticConfig
        similarity = @{
            '@odata.type' = '#Microsoft.Azure.Search.BM25Similarity'
        }
        vectorSearch = $objVectorSearchProfile
    }

    $strApiVersion = '2024-07-01'
    $strBaseUrl = "https://$($SearchServiceName).search.windows.net"
    $strIndexUrl  = "$($strBaseUrl)/indexes('$($IndexName)')?api-version=$($strApiVersion)"
    $strIndexJson = $objIndex | ConvertTo-Json -Depth 20

    if ($WhatIf) {
        Write-Host "Index '$($IndexName)' would have been created/updated with at '$($strIndexUrl)':"
        Write-Host $strIndexJson
        return $true
    }

    try {
        $strHeaders = @{
            'api-key' = (Get-SearchServiceAdminKey -ResourceGroupName $ResourceGroupName -SearchServiceName $SearchServiceName)
            'Content-Type' = 'application/json'
        }
        Invoke-AzureSearchApi -Method Put -Url $strIndexUrl -Headers $strHeaders -Body $strIndexJson
    } catch {
        Write-Error "Error invoking API to create or update Index '$IndexName'."
        return $false
    }

    return $true

}

function Update-Indexer {
    [OutputType([bool])]
    param ()

    $objIndexer = @{
        name = $IndexerName
        description = $null
        dataSourceName = $DataSourceName
        disabled = $null
        encryptionKey = $null
        fieldMappings = @()
        outputFieldMappings = @()
        parameters = @{
            batchSize = $null
            maxFailedItems = $null
            maxFailedItemsPerBatch = $null
            configuration = @{
                dataToExtract = "contentAndMetadata"
                parsingMode = "json"
            }
        }
        schedule = $null
        skillsetName = $null
        targetIndexName = $IndexName
    }

    $strApiVersion = '2024-07-01'
    $strBaseUrl = "https://$($SearchServiceName).search.windows.net"
    $strIndexerUrl  = "$($strBaseUrl)/indexers/$($IndexerName)?api-version=$($strApiVersion)"
    $strIndexerJson = $objIndexer | ConvertTo-Json -Depth 20

    if ($WhatIf) {
        Write-Host "Index '$($IndexerName)' would have been created/updated with at '$($strIndexerUrl)':"
        Write-Host $strIndexerJson
        return $true
    }

    try {
        $strHeaders = @{
            'api-key' = (Get-SearchServiceAdminKey -ResourceGroupName $ResourceGroupName -SearchServiceName $SearchServiceName)
            'Content-Type' = 'application/json'
        }
        Invoke-AzureSearchApi -Method Put -Url $strIndexerUrl -Headers $strHeaders -Body $strIndexerJson
    } catch {
        Write-Error "Error invoking API to create or update Index '$IndexerName'."
        return $false
    }

    return $true

}

# Main Logic
if (Update-DataSource) {
    Write-Host "Datasource '$DataSourceName' created/updated successfully."
}
else {
    Write-Error "Failed to create or update datasource '$DataSourceName'."
}

if (Update-Index) {
    Write-Host "Index '$IndexName' created/updated successfully."
} else {
    Write-Error "Failed to create or update index '$IndexName'."
}

if (Update-Indexer) {
    Write-Host "Indexer '$IndexerName' created/updated successfully."
} else {
    Write-Error "Failed to create or update index '$IndexerName'."
}
