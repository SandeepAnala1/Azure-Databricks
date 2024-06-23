### Interesting Fact
- Even if you are working on Azure Databricks, you can access the storage account of aws through mounting, because it just needs Client ID, Tenant ID, Credentials(secret)[Service Principle], container name & storage account name.

**Service Principal**: An identity created for use with applications, hosted services, and automated tools to access Azure resources.

**Managed Identity**: An Azure AD identity managed by Azure, automatically created and maintained for use with Azure services to access other Azure resources without needing explicit credentials.

**Difference**: Service Principals require manual management and credential handling, while Managed Identities are automatically managed by Azure and eliminate the need for credential management.

### which one is better and why?

**Managed Identity** is generally better for most use cases in **Azure** because:

1. **Automatic Management**: Azure handles the creation, management, and rotation of credentials, reducing the risk of human error and enhancing security.
2. **Simplified Configuration**: Managed Identities simplify the configuration process by eliminating the need for manual credential storage and management.
3. **Enhanced Security**: By not requiring explicit credentials, Managed Identities reduce the risk of credential leakage or misuse.
4. **Seamless Integration**: Managed Identities are designed to seamlessly integrate with other Azure services, making them ideal for a wide range of scenarios within the Azure ecosystem.

- In contrast, **Service Principals** require more manual management of credentials and configurations, making them more complex and potentially less secure if not handled properly.
-------------------------------

Outside of Azure, **Service Principals** are generally better because:

1. **Versatility**: Service Principals can be used across various environments and platforms, not limited to Azure services.
2. **Customizability**: They provide more flexibility for complex and custom configurations that might be necessary in diverse or multi-cloud environments.
3. **Cross-Platform Access**: Service Principals can authenticate to Azure resources from applications running outside Azure, such as on-premises environments or other cloud platforms.

**Managed Identities** are limited to Azure services and cannot be used directly outside the Azure environment, making Service Principals the more suitable choice for scenarios that extend beyond Azure.

--------------------------------

# Last time we hardcoded ids & keys for mounting, let's learn how can we save it in vault

## Azure Key Vault
**Key**: A cryptographic key used for encryption, decryption, or signing operations stored in Azure Key Vault.

**Secret**: Any confidential information such as passwords, connection strings, or API keys stored securely in Azure Key Vault.

**Certificate**: An SSL/TLS certificate along with its private key stored and managed in Azure Key Vault for secure communications.

- Make sure you select Vault Access Policy while creating it
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/9ca4f09d-bc96-4303-bf64-a869cbf792c0)

- Once we create secret we can get only versions
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/b4e06b52-6609-4e12-b84d-6f29009d4501)

- Now how to access this through Secret Scopes?
-----------------------------------------------

# Secret Scopes
1) Key Vault Backed Secret Scopes: Secretts are stored in Key Vault, but scope is created to access it from Databricks
3) Databricks Backed Secret Scopes: Secrets are also stored in Databricks and accessed in databricks

`https://adb-4473727248771931.11.azuredatabricks.net/#secrets/createScope` 
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/b3ba1ba6-6b33-4bc8-8581-fdf62e1dc7fc)

- DNS Name = Vault URI

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/73111900-8eb4-4a95-96f5-abfe495327ad)

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/145c377b-bea4-4680-a030-9b81100c5158)

- See here, it's not even showing the credentials, it says redacted. It is for security purposes

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/0aa92f02-354e-4197-90ca-7bbfe15b3c7b)

------------------------------------------------------------------------------------------------

# Pyspark

## File Reading Options

### RDD




























