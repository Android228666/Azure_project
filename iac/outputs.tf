output "resourceGroupName" {
  value = azurerm_resource_group.resourceGroup.name
}

output "location" {
  value = azurerm_resource_group.resourceGroup.location
}

output "storageAccountName" {
  value = azurerm_storage_account.dataLake.name
}

output "eventHubNamespaceName" {
  value = azurerm_eventhub_namespace.eventHubNamespace.name
}

output "eventHubName" {
  value = azurerm_eventhub.transactions.name
}

output "eventHubConnectionString" {
  value     = azurerm_eventhub_authorization_rule.sendListen.primary_connection_string
  sensitive = true
}

output "databricksWorkspaceName" {
  value = azurerm_databricks_workspace.databricksWorkspace.name
}

output "databricksWorkspaceUrl" {
  value = azurerm_databricks_workspace.databricksWorkspace.workspace_url
}
