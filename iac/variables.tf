variable "projectName" {
  description = "Project name used for logical naming and tags."
  type        = string
  default     = "financeAnomaly"
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "Switzerland North"
}

variable "tags" {
  description = "Common tags applied to resources."
  type        = map(string)
  default = {
    project = "financeAnomalyDetection"
    owner   = "AndriiKhomenko"
  }
}
