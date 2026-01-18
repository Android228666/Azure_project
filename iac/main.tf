resource "random_string" "suffix" {
  length  = 6
  upper   = false
  lower   = true
  numeric = true
  special = false
}

locals {
  # Azure Storage Account name constraints:
  # - 3-24 chars
  # - lowercase letters and numbers only
  storageAccountName = lower(
    substr("st${var.projectName}${random_string.suffix.result}", 0, 24)
  )

  # camelCase in Azure where allowed
  resourceGroupName      = "rgFinanceAnomaly"
  eventHubNamespaceName  = "ehFinanceAnomaly${random_string.suffix.result}"
  eventHubName           = "transactionsStream"

  # storage containers must be lowercase
  bronzeContainerName = "bronze"
  silverContainerName = "silver"
  goldContainerName   = "gold"
}

resource "azurerm_resource_group" "resourceGroup" {
  name     = local.resourceGroupName
  location = var.location
  tags     = var.tags
}

# ADLS Gen2 (Storage Account)
resource "azurerm_storage_account" "dataLake" {
  name                     = local.storageAccountName
  resource_group_name      = azurerm_resource_group.resourceGroup.name
  location                 = azurerm_resource_group.resourceGroup.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  # ADLS Gen2
  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  tags = var.tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = local.bronzeContainerName
  storage_account_name  = azurerm_storage_account.dataLake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = local.silverContainerName
  storage_account_name  = azurerm_storage_account.dataLake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = local.goldContainerName
  storage_account_name  = azurerm_storage_account.dataLake.name
  container_access_type = "private"
}

# Event Hubs Namespace (Basic tier)
resource "azurerm_eventhub_namespace" "eventHubNamespace" {
  name                = local.eventHubNamespaceName
  resource_group_name = azurerm_resource_group.resourceGroup.name
  location            = azurerm_resource_group.resourceGroup.location
  sku                 = "Basic"
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "transactions" {
  name                = local.eventHubName
  namespace_name      = azurerm_eventhub_namespace.eventHubNamespace.name
  resource_group_name = azurerm_resource_group.resourceGroup.name
  partition_count     = 2
  message_retention   = 1
}

# Auth rule for sender + receiver (connection string)
resource "azurerm_eventhub_authorization_rule" "sendListen" {
  name                = "sendListen"
  namespace_name      = azurerm_eventhub_namespace.eventHubNamespace.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.resourceGroup.name

  listen = true
  send   = true
  manage = false
}


resource "azurerm_databricks_workspace" "databricksWorkspace" {
  name                = "dbxFinanceAnomaly"
  resource_group_name = azurerm_resource_group.resourceGroup.name
  location            = azurerm_resource_group.resourceGroup.location

  sku = "standard"

  tags = var.tags
}
